//! macOS network monitor implementation.

use nix::libc::AF_ROUTE;
use socket2::{Domain, Type};
use tokio::io::unix::AsyncFd;

use crate::{BoxStream, Event, MonType, Netmon};

/// Canonical platform [`Netmon`] for macos based on `AF_ROUTE` sockets.
pub struct AfRouteMon;

impl Netmon for AfRouteMon {
    fn ty(&self) -> MonType {
        MonType::AF_ROUTE
    }

    fn event_stream(&self) -> std::io::Result<BoxStream<std::io::Result<Event>>> {
        let sock = socket2::Socket::new(Domain::from(AF_ROUTE), Type::RAW, None)?;
        sock.set_nonblocking(true)?;

        let _fd = AsyncFd::new(sock)?;

        todo!("macos netmon is currently a placeholder")
    }
}
