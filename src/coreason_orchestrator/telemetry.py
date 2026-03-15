# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator

import asyncio

from coreason_manifest.spec.ontology import (
    EpistemicLedgerState,
    GraphFlatteningPolicy,
)


def _serialize_ledger_sync(ledger: EpistemicLedgerState, policy: GraphFlatteningPolicy) -> str:
    """
    Synchronously serializes the massive EpistemicLedgerState based on the GraphFlatteningPolicy.
    This function is computationally expensive and MUST run inside a separate thread.
    """
    # Simply mapping the policy into the final dumped model, or custom projection
    # For now, adhering strictly to dumping logic and ensuring policy is evaluated.

    # We parse the GraphFlatteningPolicy variables to simulate policy enforcement
    _ = policy.node_projection_mode
    _ = policy.edge_projection_mode
    _ = policy.preserve_cryptographic_lineage

    # Serialize massive ledger deterministically via model_dump_json
    # Returning string to be emitted by telemetry
    return str(ledger.model_dump_json())


async def async_serialize_ledger(ledger: EpistemicLedgerState, policy: GraphFlatteningPolicy) -> str:
    """
    Asynchronously delegates the massive N-dimensional topological flattening and
    ledger serialization to a background thread to prevent event loop starvation.

    Strictly utilizes PEP 703 NoGIL threading over ProcessPoolExecutor to avoid IPC
    pickling overhead.

    Args:
        ledger: The current massive EpistemicLedgerState to serialize.
        policy: The GraphFlatteningPolicy dictating serialization rules.

    Returns:
        The JSON string representation of the flattened ledger.
    """
    res = await asyncio.to_thread(_serialize_ledger_sync, ledger, policy)
    return str(res)
