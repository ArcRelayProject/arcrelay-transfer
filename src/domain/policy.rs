use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ReceivePolicy {
    AskEveryTime,
    Automatic,
}

impl ReceivePolicy {
    #[must_use]
    pub const fn may_receive_silently(self, paired: bool) -> bool {
        paired && matches!(self, Self::Automatic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn automatic_receive_never_bypasses_pairing() {
        assert!(!ReceivePolicy::Automatic.may_receive_silently(false));
        assert!(ReceivePolicy::Automatic.may_receive_silently(true));
        assert!(!ReceivePolicy::AskEveryTime.may_receive_silently(true));
    }
}
