use alloc::{collections::BTreeMap, sync::Arc};
use core::time::Duration;

use futures_util::{Stream, StreamExt};
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinSet,
};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use url::Url;

use crate::{
    ControlDialer, Error,
    map_request_builder::MapRequestBuilder,
    tokio::{
        map_stream::{StateUpdate, map_stream, send_map_request},
        ping::handle_ping,
    },
};

// The control server sends a keepalive about once per minute. Match the Go client's deadline so a
// half-open connection is replaced instead of waiting forever for another map response.
const MAP_RESPONSE_TIMEOUT: Duration = Duration::from_secs(120);

/// A client to communicate with control.
#[derive(Debug)]
pub struct AsyncControlClient {
    base_url: Url,
    state_tx: broadcast::Sender<Arc<StateUpdate>>,
    command_tx: mpsc::Sender<Command>,
    _tasks: JoinSet<()>,
}

impl AsyncControlClient {
    /// Check whether it is possible to login with the given config, node keys, and auth
    /// key.
    pub async fn check_auth(
        config: &crate::Config,
        node_keys: &ts_keys::NodeState,
        auth_key: Option<&str>,
    ) -> Result<(), Error> {
        let control_url = &config.server_url;

        let h2_client = crate::tokio::connect(control_url, &node_keys.machine_keys).await?;

        crate::tokio::register(config, control_url, auth_key, node_keys, &h2_client).await?;

        Ok(())
    }

    /// Connects to the control plane, registers this Tailscale node, and starts handling the
    /// message stream from control.
    ///
    /// The second element of the return value is a netmap stream which started listening
    /// _before_ the client connected, i.e. it will not miss any updates from control.
    #[tracing::instrument(skip_all, fields(control_url = %config.server_url))]
    pub async fn connect(
        config: &crate::Config,
        node_keys: &ts_keys::NodeState,
        auth_key: Option<&str>,
    ) -> Result<
        (
            Self,
            impl Stream<Item = Arc<StateUpdate>> + Send + Sync + use<>,
        ),
        Error,
    > {
        let control_url = &config.server_url;
        let mut tasks = JoinSet::new();

        let h2_client = crate::tokio::connect(control_url, &node_keys.machine_keys).await?;
        tracing::info!("connected to control, registering");

        crate::tokio::register(config, control_url, auth_key, node_keys, &h2_client).await?;

        tracing::info!("registered, starting netmap stream");

        let builder = MapRequestBuilder::new(node_keys)
            .keep_alive(true)
            .omit_peers(false)
            .stream(true);

        let mut request = if let Some(hostname) = &config.hostname {
            builder.hostname(hostname)
        } else {
            builder
        }
        .build();

        let client_name = config.format_client_name();
        let host_info = request.host_info.get_or_insert_default();
        host_info.app = &client_name;
        host_info.ipn_version = crate::PKG_VERSION;

        let (state_tx, state_rx) = broadcast::channel(32);
        let (command_tx, command_rx) = mpsc::channel(32);

        tasks.spawn({
            let state_tx = state_tx.clone();
            let control_url = control_url.clone();
            let node_keys = node_keys.clone();
            let auth_key = auth_key.map(ToOwned::to_owned);
            let config = config.clone();

            async move {
                run(
                    state_tx,
                    command_rx,
                    control_url.clone(),
                    node_keys.clone(),
                    auth_key,
                    config,
                )
                .await
            }
        });

        Ok((
            Self {
                base_url: control_url.clone(),
                state_tx,
                command_tx,
                _tasks: tasks,
            },
            netmap_stream(state_rx),
        ))
    }

    /// Set the DERP home region for this node.
    #[tracing::instrument(skip_all, fields(map_url = %self.map_url(), %region_id), level = "trace")]
    pub async fn set_home_region<'c>(
        &mut self,
        region_id: ts_derp::RegionId,
        latencies: impl IntoIterator<Item = (&'c str, f64)>,
    ) {
        tracing::trace!(region = %region_id, "reporting home derp to control server");

        if let Err(e) = self
            .command_tx
            .send(Command::SetDerpHomeRegion {
                id: region_id,
                latencies: latencies
                    .into_iter()
                    .map(|(name, sample)| (name.to_owned(), sample))
                    .collect(),
            })
            .await
        {
            tracing::error!(error = %e, "setting home derp region");
        }
    }

    /// Construct the URL that should be used to fetch the netmap.
    pub fn map_url(&self) -> Url {
        self.base_url
            .join("machine/map")
            .expect("map_url was parsed without issue before")
    }

    /// Get a stream of all netmap updates.
    pub fn netmap_stream(&self) -> impl Stream<Item = Arc<StateUpdate>> + Send + Sync + use<> {
        netmap_stream(self.state_tx.subscribe())
    }
}

#[derive(Debug)]
pub enum Command {
    SetDerpHomeRegion {
        id: ts_derp::RegionId,
        latencies: BTreeMap<String, f64>,
    },
}

pub async fn run(
    state_tx: broadcast::Sender<Arc<StateUpdate>>,
    mut command_rx: mpsc::Receiver<Command>,
    control_url: Url,
    node_keys: ts_keys::NodeState,
    auth_key: Option<String>,
    config: crate::Config,
) {
    let mut dialer = ControlDialer::default();

    loop {
        match run_once(
            &state_tx,
            &mut command_rx,
            &control_url,
            &node_keys,
            auth_key.as_deref(),
            &config,
            &mut dialer,
        )
        .await
        {
            // TODO(npry): netmap stream resumption on reconnect
            Ok(()) => {
                tracing::warn!("netmap stream ended without error, attempting restart");
            }
            Err(e) => {
                tracing::error!(error = %e, "netmap stream failed, attempting restart");
                tokio::time::sleep(core::time::Duration::from_millis(500)).await;
            }
        }
    }
}

async fn run_once(
    state_tx: &broadcast::Sender<Arc<StateUpdate>>,
    command_rx: &mut mpsc::Receiver<Command>,
    control_url: &Url,
    node_keys: &ts_keys::NodeState,
    auth_key: Option<&str>,
    config: &crate::Config,
    control_dialer: &mut ControlDialer,
) -> Result<(), Error> {
    let h2_client = control_dialer
        .full_connect_next(control_url, &node_keys.machine_keys)
        .await?;

    crate::tokio::register(config, control_url, auth_key, node_keys, &h2_client).await?;

    let builder = MapRequestBuilder::new(node_keys)
        .keep_alive(true)
        .omit_peers(false)
        .stream(true);

    let request = if let Some(hostname) = &config.hostname {
        builder.hostname(hostname)
    } else {
        builder
    }
    .build();

    let map_url = control_url.join("machine/map").unwrap();

    let reader = send_map_request(request, &map_url, &h2_client).await?;

    let mut stream = core::pin::pin!(map_stream(reader));
    tracing::info!("netmap stream started");

    let mut last_map_response_at = tokio::time::Instant::now();

    loop {
        let state_update = {
            let response = next_map_response(
                &mut stream,
                last_map_response_at,
                MAP_RESPONSE_TIMEOUT,
            );
            tokio::pin!(response);

            loop {
                tokio::select! {
                    state_update = &mut response => break state_update,
                    command = command_rx.recv() => {
                        match command.unwrap() {
                            Command::SetDerpHomeRegion { id, latencies } => {
                                let mut builder = MapRequestBuilder::new(node_keys)
                                    .keep_alive(false)
                                    .omit_peers(true)
                                    .stream(false)
                                    .preferred_derp(id)
                                    .derp_latencies(latencies.iter().map(|(k, v)| (k.as_str(), *v)));

                                if let Some(hostname) = &config.hostname {
                                    builder = builder.hostname(hostname);
                                }
                                let req = builder.build();

                                drop(send_map_request(req, &map_url, &h2_client).await?);
                            },
                        }
                    }
                }
            }
        };

        let Some(state_update) = state_update? else {
            break;
        };
        last_map_response_at = tokio::time::Instant::now();

        let _ = handle_ping(&state_update, control_url, &h2_client).await;

        if let Some(dial_plan) = &state_update.dial_plan
            && control_dialer.update_dial_plan(dial_plan)
        {
            tracing::trace!(new_dial_plan = ?dial_plan);
        }

        // This errors only if there are no receivers. That's not semantically an error for
        // us, so just ignore it.
        let _ignore = state_tx.send(Arc::new(state_update));
    }

    Ok(())
}

async fn next_map_response<S>(
    stream: &mut S,
    last_response_at: tokio::time::Instant,
    timeout: Duration,
) -> Result<Option<S::Item>, Error>
where
    S: Stream + Unpin,
{
    tokio::time::timeout_at(last_response_at + timeout, stream.next())
        .await
        .map_err(|_| {
            tracing::warn!(?timeout, "timed out waiting for a map response");
            Error::NetworkError(crate::Operation::MapRequest)
        })
}

fn netmap_stream(
    rx: broadcast::Receiver<Arc<StateUpdate>>,
) -> impl Stream<Item = Arc<StateUpdate>> + Send + Sync {
    tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(async |x| {
        if let Err(BroadcastStreamRecvError::Lagged(n)) = &x {
            tracing::warn!(messages_missed = n, "map_stream lagged");
        }

        x.ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn stalled_map_stream_hits_watchdog() {
        let mut stream = futures_util::stream::pending::<()>();
        let timeout = core::time::Duration::from_secs(120);
        let started_at = tokio::time::Instant::now();

        let error = next_map_response(&mut stream, started_at, timeout)
            .await
            .unwrap_err();

        assert_eq!(error, Error::NetworkError(crate::Operation::MapRequest));
        assert_eq!(tokio::time::Instant::now() - started_at, timeout);
    }

    #[tokio::test(start_paused = true)]
    async fn map_responses_reset_watchdog() {
        let (response_tx, response_rx) = tokio::sync::mpsc::channel(2);
        let response_tx_guard = response_tx.clone();
        tokio::spawn(async move {
            for response in [1, 2] {
                tokio::time::sleep(core::time::Duration::from_secs(90)).await;
                response_tx.send(response).await.unwrap();
            }
        });

        let mut stream = tokio_stream::wrappers::ReceiverStream::new(response_rx);
        let timeout = core::time::Duration::from_secs(120);
        let started_at = tokio::time::Instant::now();
        let mut last_response_at = started_at;

        assert_eq!(
            next_map_response(&mut stream, last_response_at, timeout)
                .await
                .unwrap(),
            Some(1)
        );
        last_response_at = tokio::time::Instant::now();
        assert_eq!(
            next_map_response(&mut stream, last_response_at, timeout)
                .await
                .unwrap(),
            Some(2)
        );
        last_response_at = tokio::time::Instant::now();
        assert_eq!(
            tokio::time::Instant::now() - started_at,
            core::time::Duration::from_secs(180)
        );
        assert_eq!(
            next_map_response(&mut stream, last_response_at, timeout)
                .await
                .unwrap_err(),
            Error::NetworkError(crate::Operation::MapRequest)
        );
        assert_eq!(
            tokio::time::Instant::now() - started_at,
            core::time::Duration::from_secs(300)
        );

        drop(response_tx_guard);
    }

    #[tokio::test(start_paused = true)]
    async fn command_activity_does_not_postpone_watchdog() {
        let mut stream = futures_util::stream::pending::<()>();
        let timeout = core::time::Duration::from_secs(120);
        let started_at = tokio::time::Instant::now();
        let response = next_map_response(&mut stream, started_at, timeout);
        tokio::pin!(response);

        for _ in 0..3 {
            tokio::select! {
                result = &mut response => {
                    panic!("map response wait completed early: {result:?}");
                }
                () = tokio::time::sleep(core::time::Duration::from_secs(30)) => {}
            }
        }

        let error = response.await.unwrap_err();

        assert_eq!(error, Error::NetworkError(crate::Operation::MapRequest));
        assert_eq!(tokio::time::Instant::now() - started_at, timeout);
    }
}
