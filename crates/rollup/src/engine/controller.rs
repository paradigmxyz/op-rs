//! The engine controller keeps the state of the engine.

use op_alloy_protocol::L2BlockInfo;

/// The engine controller.
#[derive(Debug, Clone)]
pub struct EngineController {
    /// The block head state.
    pub unsafe_head: L2BlockInfo,
    /// A cross unsafe head.
    pub cross_unsafe_head: L2BlockInfo,
    /// The pending local safe head.
    pub pending_local_safe_head: L2BlockInfo, 
    /// The local safe head.
    pub local_safe_head: L2BlockInfo,
    /// Derived from L1 and cross-verified.
    pub safe_head: L2BlockInfo,
    /// The finalized safe head.
    pub finalized_head: L2BlockInfo,
}
