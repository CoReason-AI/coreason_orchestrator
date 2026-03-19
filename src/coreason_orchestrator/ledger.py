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
    BeliefMutationEvent,
    DefeasibleCascadeEvent,
    EpistemicLedgerState,
    ExecutionNodeReceipt,
    ObservationEvent,
    RollbackIntent,
    StateDifferentialManifest,
    StateMutationIntent,
    TamperFaultEvent,
)
from coreason_manifest.utils.algebra import verify_merkle_proof

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
    # 1. Enforce Cryptographic Integrity on Execution Traces
    if (
        isinstance(event, list)
        and len(event) > 0
        and isinstance(event[0], ExecutionNodeReceipt)
        and not verify_merkle_proof(event)
    ):
        raise TamperFaultEvent("Merkle-DAG validation failed. Trace is cryptographically compromised.")

    # 2. Basic Zero-Knowledge Proof sanity check
    if (
        hasattr(event, "zk_proof")
        and event.zk_proof is not None
        and not getattr(event.zk_proof, "public_inputs_hash", None)
    ):
        raise TamperFaultEvent("ZK-Proof missing public inputs hash.")

    # 3. Synthesize the mathematically bound event using EventFactory to ensure
    # the RFC 8785 hash is deterministically bound prior to final instantiation.
    # We dump the passed in event's parameters to rebuild it strictly through the factory
    event_kwargs = event.model_dump(exclude={"event_id"})
    crystallized_event = EventFactory.build_event(type(event), **event_kwargs)

    # 4. Check for automated Truth Maintenance if Epistemic Contraction is required.
    # An ObservationEvent or BeliefMutationEvent with "falsified_node_id" in its payload triggers this.
    cascade_manifest: StateDifferentialManifest | None = None
    if isinstance(event, (ObservationEvent, BeliefMutationEvent)):
        payload = event.payload
        if isinstance(payload, dict):
            falsified_node_id = payload.get("falsified_node_id")
            if isinstance(falsified_node_id, str):
                # We have a logical contradiction. Crawl the Merkle-DAG for downstream edges to quarantine.
                quarantined_event_ids: list[str] = []
                # Initialize queue with the root falsified node ID
                falsified_queue: list[str] = [falsified_node_id]
                visited_nodes: set[str] = set()

                # Prevent infinite loops in cyclic dependencies
                max_depth = getattr(ledger, "max_depth", 100)
                max_loops = getattr(ledger, "max_loops", 1000)
                loops = 0
                current_depth = 0

                while falsified_queue and current_depth < max_depth and loops < max_loops:
                    current_depth += 1
                    next_queue: list[str] = []

                    for current_falsified_node in falsified_queue:
                        if current_falsified_node in visited_nodes:
                            continue
                        visited_nodes.add(current_falsified_node)

                        for history_event in ledger.history:
                            loops += 1
                            if loops >= max_loops:
                                break

                            # Semantic edges might be stored as nested dictionaries in BeliefMutationEvent payloads
                            if isinstance(history_event, BeliefMutationEvent):
                                if isinstance(history_event.payload, dict):
                                    embedded_edges = history_event.payload.get("embedded_edges", [])
                                if isinstance(embedded_edges, list):
                                    for edge_dict in embedded_edges:
                                        if isinstance(edge_dict, dict):
                                            subj = edge_dict.get("subject_node_id")
                                            obj = edge_dict.get("object_node_id")
                                            edge_id = edge_dict.get("edge_id")

                                            is_subj = subj == current_falsified_node
                                            is_obj = obj == current_falsified_node
                                            is_targeted = is_subj or is_obj

                                            is_valid_edge = isinstance(edge_id, str)
                                            if is_targeted and is_valid_edge and edge_id not in quarantined_event_ids:
                                                quarantined_event_ids.append(str(edge_id))
                                                # Recursively taint the target of the causal edge
                                                if isinstance(obj, str) and obj != current_falsified_node:
                                                    next_queue.append(obj)
                                                if isinstance(subj, str) and subj != current_falsified_node:
                                                    next_queue.append(subj)

                    falsified_queue = next_queue

                # Synthesize intents strictly adhering to Anti-CRUD and preventing O(N^2) memory
                if quarantined_event_ids:
                    cascade = DefeasibleCascadeEvent(
                        cascade_id="placeholder",
                        root_falsified_event_id=falsified_node_id,
                        propagated_decay_factor=1.0,
                        quarantined_event_ids=quarantined_event_ids,
                    )

                    # We compute a deterministic rollback to apply the cascade
                    rollback = RollbackIntent(
                        request_id="auto_truth_maintenance",
                        target_event_id=falsified_node_id,
                        invalidated_node_ids=[falsified_node_id],
                    )

                    # Apply rollback natively yields a StateDifferentialManifest correctly
                    cascade_manifest = apply_rollback(ledger, rollback, cascade)

    # 5. Append the crystallized event (and potentially the cascade) to the history array
    # Since ledger is an immutable snapshot, we create a new ledger object.
    if cascade_manifest:
        new_history = (*ledger.history, crystallized_event, cascade_manifest)
    else:
        new_history = (*ledger.history, crystallized_event)

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

    # Explicitly isolate the falsified subgraph to prevent epistemic contagion
    patches.extend(
        StateMutationIntent(op="add", path="/retracted_nodes/-", value=node_id)
        for node_id in cascade.quarantined_event_ids
    )

    return EventFactory.build_event(
        StateDifferentialManifest,
        diff_id="placeholder",  # Will be replaced by EventFactory computing the true hash
        author_node_id="did:coreason:orchestrator",
        lamport_timestamp=0,
        vector_clock={},
        patches=patches,
    )
