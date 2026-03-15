# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator

import copy
from typing import Any

from coreason_manifest.spec.ontology import (
    DefeasibleCascadeEvent,
    EpistemicLedgerState,
    RollbackIntent,
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
) -> EpistemicLedgerState:
    """
    Applies a topological rollback by appending the corresponding RollbackIntent
    and DefeasibleCascadeEvent strictly adhering to Anti-CRUD philosophies.

    This mathematically isolates falsified branches by recording the invalidation rather
    than mutating historical arrays in-place.

    Args:
        ledger: The current crystallized EpistemicLedgerState.
        rollback: The structured intent identifying the branch to invalidate.
        cascade: The calculated state differential payload muting specific subgraphs.

    Returns:
        A completely synthesized new instance of EpistemicLedgerState containing the update.
    """
    # Create independent copies to enforce strict mathematical immutability boundaries
    new_rollback = copy.deepcopy(rollback)
    new_cascade = copy.deepcopy(cascade)

    # We must synthesize completely new state arrays instead of appending in-place
    # active_rollbacks and active_cascades may be None based on default pydantic values if optional
    current_rollbacks = ledger.active_rollbacks if ledger.active_rollbacks is not None else []
    current_cascades = ledger.active_cascades if ledger.active_cascades is not None else []

    new_active_rollbacks = list(current_rollbacks)
    new_active_rollbacks.append(new_rollback)

    new_active_cascades = list(current_cascades)
    new_active_cascades.append(new_cascade)

    return ledger.model_copy(
        update={
            "active_rollbacks": new_active_rollbacks,
            "active_cascades": new_active_cascades,
        }
    )
