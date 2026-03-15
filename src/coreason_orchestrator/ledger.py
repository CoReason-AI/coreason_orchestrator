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

from coreason_manifest.spec.ontology import AnyStateEvent, EpistemicLedgerState

from coreason_orchestrator.utils.crypto import calculate_event_hash


def append_event(ledger: EpistemicLedgerState, event: AnyStateEvent) -> EpistemicLedgerState:
    """
    Appends a new state event to the epistemic ledger enforcing the immutable append-only
    Merkle-DAG causal chain.

    This function does NOT mutate the original `ledger` or `event`. It strictly adheres to
    "Passive by Design" and "Anti-CRUD" principles. It mathematically forces the new event's
    `event_id` to its JSON Control Hash (RFC 8785) prior to inclusion.

    Args:
        ledger: The current crystallized EpistemicLedgerState.
        event: The newly synthesized AnyStateEvent.

    Returns:
        A new instance of EpistemicLedgerState with the mathematically bound event appended.
    """
    # Create an independent copy to strictly enforce Anti-CRUD and non-mutation
    new_event = copy.deepcopy(event)

    # 1. Synthesize the new AnyStateEvent (handled via argument)
    # 2. Assign the calculated RFC 8785 JSON Control Hash strictly to the event_id property
    # We must ensure the hash calculation is deterministic and doesn't depend on the
    # placeholder event_id itself. A standard approach is to nullify the event_id
    # before hashing.
    temp_event = new_event.model_copy(update={"event_id": ""})
    new_event_hash = calculate_event_hash(temp_event)

    new_event = new_event.model_copy(update={"event_id": new_event_hash})

    # 3. Append the crystallized event to the history array
    # Since ledger is an immutable snapshot, we create a new ledger object.
    new_history = list(ledger.history)
    new_history.append(new_event)

    return ledger.model_copy(update={"history": new_history})
