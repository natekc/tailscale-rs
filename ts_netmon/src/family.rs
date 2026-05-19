use core::fmt::{Debug, Formatter};

/// Specification of IPv4 or IPv6 or both.
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum FamilyOrBoth {
    /// Both address families; `AF_UNSPEC`.
    Both,
    /// A single address family.
    Single(Family),
}

impl Debug for FamilyOrBoth {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FamilyOrBoth::Both => write!(f, "Both"),
            FamilyOrBoth::Single(family) => family.fmt(f),
        }
    }
}

impl From<Family> for FamilyOrBoth {
    fn from(family: Family) -> Self {
        FamilyOrBoth::Single(family)
    }
}

/// Specification of either IPv4 or IPv6.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Family {
    /// The IPv4 address family.
    Ipv4,
    /// The IPv6 address family.
    Ipv6,
}

impl TryFrom<FamilyOrBoth> for Family {
    type Error = ();

    fn try_from(value: FamilyOrBoth) -> Result<Self, Self::Error> {
        match value {
            FamilyOrBoth::Both => Err(()),
            FamilyOrBoth::Single(family) => Ok(family),
        }
    }
}
