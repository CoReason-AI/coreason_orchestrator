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
    EpistemicLedgerState,
    StateHydrationManifest,
)


def compile_state_hydration(
    ledger: EpistemicLedgerState,
    coordinate: str,
    context: dict[str, Any],
    max_tokens: int,
) -> StateHydrationManifest:
    """
    Compiles a lightweight IPC-friendly state hydration manifest.

    This function strictly extracts cryptographic CIDs from the ledger's
    history to prevent massive object serialization over process boundaries,
    adhering to the Hollow Data Plane constraints.

    Args:
        ledger: The current crystallized EpistemicLedgerState.
        coordinate: A string ID representing the session or specific spatial trace binding.
        context: A strictly typed dictionary for ephemeral context variables.
        max_tokens: An integer representing the physical limit of the context window.

    Returns:
        A completely synthesized new instance of StateHydrationManifest.
    """
    # Extract only the deterministic CIDs from the validated append-only history
    # The list generation naturally synthesizes a new list structure.
    cids = [event.event_id for event in ledger.history]

    # Synthesize the StateHydrationManifest
    return StateHydrationManifest(
        epistemic_coordinate=coordinate,
        crystallized_ledger_cids=cids,
        working_context_variables=context,
        max_retained_tokens=max_tokens,
    )
