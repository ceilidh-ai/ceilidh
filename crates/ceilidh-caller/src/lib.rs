//! ceilidh-caller: the session and turn state machine.
//!
//! Owned by lane/caller-server. Pure logic lives here (transition rules,
//! claim eligibility, affinity); storage and HTTP live in ceilidh-server.

use ceilidh_protocol::TurnStatus;

/// The legal turn transitions. Anything else is a bug in the caller.
pub fn transition_allowed(from: TurnStatus, to: TurnStatus) -> bool {
    use TurnStatus::*;
    matches!(
        (from, to),
        (Queued, Claimed)
            | (Claimed, Working)
            // A turn that streamed nothing never reaches Working, and still
            // has to be able to finish.
            | (Claimed, Done)
            | (Claimed, Capped)
            | (Claimed, Error)
            | (Working, Done)
            | (Working, Error)
            | (Working, Capped)
            // A runner that dies mid-flight has its turn requeued.
            | (Claimed, Queued)
            | (Working, Queued)
            | (Capped, Queued)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ceilidh_protocol::TurnStatus::*;

    #[test]
    fn happy_path_is_legal() {
        assert!(transition_allowed(Queued, Claimed));
        assert!(transition_allowed(Claimed, Working));
        assert!(transition_allowed(Working, Done));
    }

    #[test]
    fn a_silent_turn_can_still_finish() {
        // No chunks means the turn never left Claimed.
        assert!(transition_allowed(Claimed, Done));
        assert!(transition_allowed(Claimed, Capped));
    }

    #[test]
    fn abandoned_work_can_be_requeued() {
        assert!(transition_allowed(Claimed, Queued));
        assert!(transition_allowed(Working, Queued));
    }

    #[test]
    fn done_is_terminal() {
        assert!(!transition_allowed(Done, Queued));
        assert!(!transition_allowed(Done, Working));
    }
}
