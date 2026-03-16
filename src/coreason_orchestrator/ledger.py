# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator

from typing import Any

from coreason_manifest.spec.ontology import (
    DefeasibleCascadeEvent,
    EpistemicLedgerState,
    RollbackIntent,
    StateDifferentialManifest,
    StateMutationIntent,
)

from coreason_orchestrator.factory import EventFactory


def append_event(ledger: EpistemicLedgerState, event: Any) -> EpistemicLedgerState:
    """
    Appends a new state event or intent to the epistemic ledger enforcing the immutable append-only
    Merkle-DAG causal chain.

    This function does NOT mutate the original `ledger` or `event`. It strictly adheres to
    "Passive by Design" and "Anti-CRUD" principles. It mathematically forces the new event's
    `event_id` to its JSON Control Hash (RFC 8785) prior to inclusion.

    Args:
        ledger: The current crystallized EpistemicLedgerState.
        event: The newly synthesized AnyStateEvent or AnyIntent.

    Returns:
        A new instance of EpistemicLedgerState with the mathematically bound event appended.
    """
    # 1. Synthesize the mathematically bound event using EventFactory to ensure
    # the RFC 8785 hash is deterministically bound prior to final instantiation.
    # We dump the passed in event's parameters to rebuild it strictly through the factory
    event_kwargs = event.model_dump(exclude={"event_id"})
    crystallized_event = EventFactory.build_event(type(event), **event_kwargs)

    # 2. Append the crystallized event to the history array
    # Since ledger is an immutable snapshot, we create a new ledger object.
    new_history = list(ledger.history)
    new_history.append(crystallized_event)

    return ledger.model_copy(update={"history": new_history})


def apply_rollback(
    ledger: EpistemicLedgerState, rollback: RollbackIntent, cascade: DefeasibleCascadeEvent
) -> StateDifferentialManifest:
    """
    Applies a topological rollback by synthesizing a StateDifferentialManifest
    strictly adhering to Anti-CRUD philosophies and preventing O(N^2) memory
    allocations from deep-copying immutable ledger arrays in-place.

    This mathematically isolates falsified branches by recording the invalidation
    as a deterministically bound state differential rather than mutating historical
    arrays or forcing a full EpistemicLedgerState re-instantiation.

    Args:
        ledger: The current crystallized EpistemicLedgerState.
        rollback: The structured intent identifying the branch to invalidate.
        cascade: The calculated state differential payload muting specific subgraphs.

    Returns:
        A synthesized StateDifferentialManifest representing the exact state mutation.
    """
    _ = ledger  # Ledger parameter kept for API signature compatibility, but unused to prevent N^2 copying

    # Prevent O(N^2) memory allocations by creating a StateDifferentialManifest
    # with the appropriate JSON patches instead of copying the whole array.
    patches = [
        StateMutationIntent(op="add", path="/active_rollbacks/-", value=rollback.model_dump()),
        StateMutationIntent(op="add", path="/active_cascades/-", value=cascade.model_dump()),
    ]

    return EventFactory.build_event(
        StateDifferentialManifest,
        diff_id="placeholder",  # Will be replaced by EventFactory computing the true hash
        author_node_id="did:coreason:orchestrator",
        lamport_timestamp=0,
        vector_clock={},
        patches=patches,
    )
