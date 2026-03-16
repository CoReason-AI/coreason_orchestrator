from typing import Any

import hypothesis.strategies as st

# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator
from coreason_manifest.spec.ontology import (
    AgentNodeProfile,
    CouncilTopologyManifest,
    DAGTopologyManifest,
    EpistemicFlowStateReceipt,
    EpistemicLedgerState,
    EpistemicProvenanceReceipt,
    ObservationEvent,
    SystemNodeProfile,
    WorkflowManifest,
)
from hypothesis import given

from coreason_orchestrator.resolve import resolve_current_node


def get_mock_workflow() -> WorkflowManifest:
    """Creates a basic WorkflowManifest for testing."""
    node_a = AgentNodeProfile(description="A", architectural_intent=".", justification=".", type="agent")
    node_b = AgentNodeProfile(description="B", architectural_intent=".", justification=".", type="agent")

    topology = DAGTopologyManifest(
        max_depth=5,
        edges=[("did:coreason:node:a", "did:coreason:node:b")],
        nodes={"did:coreason:node:a": node_a, "did:coreason:node:b": node_b},
        max_fan_out=5,
    )

    return WorkflowManifest(
        manifest_version="1.0.0",
        topology=topology,
        genesis_provenance=EpistemicProvenanceReceipt(extracted_by="did:coreason:node:a", source_event_id="dummy"),
    )


def test_resolve_current_node_epistemic_flow_state_receipt() -> None:
    """Verifies that an EpistemicFlowStateReceipt correctly completes a node and advances the frontier."""
    workflow = get_mock_workflow()

    event = EpistemicFlowStateReceipt(
        event_id="e1",
        timestamp=1.0,
        type="epistemic_flow_state",
        source_trajectory_id="did:coreason:node:a",
        estimated_flow_value=1.0,
        terminal_reward_factorized=True,
    )
    ledger = EpistemicLedgerState(history=[event])

    frontier = resolve_current_node(workflow, ledger)
    assert len(frontier) == 1
    assert frontier[0].description == "B"


@given(source_trajectory_id=st.from_regex(r"^did:[a-z0-9]+:[a-zA-Z0-9.\-_:]+$", fullmatch=True))  # type: ignore[misc]
def test_resolve_current_node_epistemic_flow_state_receipt_hypothesis(source_trajectory_id: str) -> None:
    """Verifies that EpistemicFlowStateReceipt correctly handles trajectory IDs with regex properties."""
    node_a = AgentNodeProfile(description="A", architectural_intent=".", justification=".", type="agent")
    node_b = AgentNodeProfile(description="B", architectural_intent=".", justification=".", type="agent")

    topology = DAGTopologyManifest(
        max_depth=5,
        edges=[(source_trajectory_id, "did:coreason:node:b")],
        nodes={source_trajectory_id: node_a, "did:coreason:node:b": node_b},
        max_fan_out=5,
    )

    workflow = WorkflowManifest(
        manifest_version="1.0.0",
        topology=topology,
        genesis_provenance=EpistemicProvenanceReceipt(extracted_by="did:coreason:sys", source_event_id="dummy"),
    )

    event = EpistemicFlowStateReceipt(
        event_id="e1",
        timestamp=1.0,
        type="epistemic_flow_state",
        source_trajectory_id=source_trajectory_id,
        estimated_flow_value=1.0,
        terminal_reward_factorized=True,
    )
    ledger = EpistemicLedgerState(history=[event])

    frontier = resolve_current_node(workflow, ledger)
    assert len(frontier) == 1
    assert frontier[0].description == "B"


def test_resolve_current_node_no_routing_manifest() -> None:
    """Verifies that the resolver correctly traverses without a DynamicRoutingManifest."""
    workflow = get_mock_workflow()
    ledger = EpistemicLedgerState(history=[])

    frontier = resolve_current_node(workflow, ledger)
    assert len(frontier) == 1
    assert frontier[0].description == "A"


def test_resolve_current_node_with_valid_bypassed_steps() -> None:
    """Verifies that a valid bypassed_steps payload correctly advances the frontier."""
    workflow = get_mock_workflow()

    # Payload simulates bypassing node A, advancing to node B
    payload: dict[str, Any] = {"bypassed_steps": [{"bypassed_node_id": "did:coreason:node:a"}]}
    event = ObservationEvent(event_id="e1", timestamp=1.0, type="observation", payload=payload)  # type: ignore[arg-type]
    ledger = EpistemicLedgerState(history=[event])

    frontier = resolve_current_node(workflow, ledger)
    assert len(frontier) == 1
    assert frontier[0].description == "B"


def test_resolve_current_node_with_invalid_bypassed_steps() -> None:
    """Verifies that an invalid bypassed_steps payload (non-string types) safely ignores errors."""
    workflow = get_mock_workflow()

    # Payload simulates bypassing but with invalid node_id types
    payload: dict[str, Any] = {
        "bypassed_steps": [
            {"bypassed_node_id": 123},
            {"bypassed_node_id": None},
            {"bypassed_node_id": ["list"]},
            {"invalid_key": "did:coreason:node:a"},
            123,
            "string",
        ]
    }
    event = ObservationEvent(event_id="e1", timestamp=1.0, type="observation", payload=payload)  # type: ignore[arg-type]
    ledger = EpistemicLedgerState(history=[event])

    # Since none of the valid bypassed_node_id strings are found, it should still be on node A
    frontier = resolve_current_node(workflow, ledger)
    assert len(frontier) == 1
    assert frontier[0].description == "A"


def test_resolve_current_node_with_valid_active_subgraphs() -> None:
    """Verifies that a valid active_subgraphs payload restricts the routing frontier."""
    node_a = AgentNodeProfile(description="A", architectural_intent=".", justification=".", type="agent")
    node_b1 = AgentNodeProfile(description="B1", architectural_intent=".", justification=".", type="agent")
    node_b2 = AgentNodeProfile(description="B2", architectural_intent=".", justification=".", type="agent")

    topology = DAGTopologyManifest(
        max_depth=5,
        edges=[("did:coreason:node:a", "did:coreason:node:b1"), ("did:coreason:node:a", "did:coreason:node:b2")],
        nodes={"did:coreason:node:a": node_a, "did:coreason:node:b1": node_b1, "did:coreason:node:b2": node_b2},
        max_fan_out=5,
    )
    workflow = WorkflowManifest(
        manifest_version="1.0.0",
        genesis_provenance=EpistemicProvenanceReceipt(extracted_by="did:coreason:node:a", source_event_id="dummy"),
        topology=topology,
    )

    # Node A is completed (via standard observation source mapping)
    # AND active_subgraphs restricts the next steps to ONLY B2
    payload = {"active_subgraphs": {"group1": ["did:coreason:node:b2"]}}
    event = ObservationEvent(
        event_id="e1",
        timestamp=1.0,
        type="observation",
        source_node_id="did:coreason:node:a",
        payload=payload,  # type: ignore[arg-type]
    )
    ledger = EpistemicLedgerState(history=[event])

    frontier = resolve_current_node(workflow, ledger)
    assert len(frontier) == 1
    assert frontier[0].description == "B2"


def test_resolve_current_node_with_invalid_active_subgraphs() -> None:
    """Verifies that an invalid active_subgraphs payload safely ignores type errors."""
    node_a = AgentNodeProfile(description="A", architectural_intent=".", justification=".", type="agent")
    node_b1 = AgentNodeProfile(description="B1", architectural_intent=".", justification=".", type="agent")
    node_b2 = AgentNodeProfile(description="B2", architectural_intent=".", justification=".", type="agent")

    topology = DAGTopologyManifest(
        max_depth=5,
        edges=[("did:coreason:node:a", "did:coreason:node:b1"), ("did:coreason:node:a", "did:coreason:node:b2")],
        nodes={"did:coreason:node:a": node_a, "did:coreason:node:b1": node_b1, "did:coreason:node:b2": node_b2},
        max_fan_out=5,
    )
    workflow = WorkflowManifest(
        manifest_version="1.0.0",
        genesis_provenance=EpistemicProvenanceReceipt(extracted_by="did:coreason:node:a", source_event_id="dummy"),
        topology=topology,
    )

    # Payload has invalid types in active_subgraphs
    payload: dict[str, Any] = {
        "active_subgraphs": {
            "group1": [123, None, {"dict": "yes"}],
            "group2": "not a list",
            "123": ["did:coreason:node:b1"],  # invalid key type, although dict.values() bypasses keys
        }
    }
    event = ObservationEvent(
        event_id="e1",
        timestamp=1.0,
        type="observation",
        source_node_id="did:coreason:node:a",
        payload=payload,  # type: ignore[arg-type]
    )
    ledger = EpistemicLedgerState(history=[event])

    frontier = resolve_current_node(workflow, ledger)

    # Since active_subgraphs_set is populated with ONLY valid strings, and here it is either empty
    # or contains valid items from dict.values(). Wait! `group2` is a string (not list),
    # `123` is a valid dict key in python (but JSON parsed it's a string key).
    # Since the `123` list contains valid strings, `did:coreason:node:b1` WILL be added.
    # Therefore, B1 should be returned. B2 should NOT be returned.

    assert len(frontier) == 1
    assert frontier[0].description == "B1"


def test_resolve_current_node_empty_or_non_dag() -> None:
    """Verifies that empty nodes or non-DAG topologies are handled securely."""
    # Empty nodes
    topology = DAGTopologyManifest(max_depth=5, edges=[], nodes={}, max_fan_out=5, lifecycle_phase="draft")
    workflow = WorkflowManifest(
        manifest_version="1.0.0",
        genesis_provenance=EpistemicProvenanceReceipt(extracted_by="did:coreason:node:a", source_event_id="dummy"),
        topology=topology,
    )
    ledger = EpistemicLedgerState(history=[])
    frontier = resolve_current_node(workflow, ledger)
    assert frontier == []

    # Cyclic non-DAG fallback behavior
    node_a = AgentNodeProfile(description="A", architectural_intent=".", justification=".", type="agent")
    topology = DAGTopologyManifest(
        max_depth=5,
        edges=[("did:coreason:node:a", "did:coreason:node:a")],  # Cycle
        nodes={"did:coreason:node:a": node_a},
        max_fan_out=5,
        lifecycle_phase="draft",
    )
    workflow = WorkflowManifest(
        manifest_version="1.0.0",
        genesis_provenance=EpistemicProvenanceReceipt(extracted_by="did:coreason:node:a", source_event_id="dummy"),
        topology=topology,
    )
    frontier = resolve_current_node(workflow, ledger)
    assert len(frontier) == 1
    assert frontier[0].description == "A"


def test_resolve_current_node_non_dag() -> None:
    node_a = AgentNodeProfile(description="A", architectural_intent=".", justification=".", type="agent")
    topology = CouncilTopologyManifest(
        nodes={"did:coreason:node:a": node_a},
        shared_state_contract=None,
        information_flow=None,
        type="council",
        adjudicator_id="did:coreason:node:a",
    )
    workflow = WorkflowManifest(
        manifest_version="1.0.0",
        genesis_provenance=EpistemicProvenanceReceipt(extracted_by="did:coreason:sys", source_event_id="dummy"),
        topology=topology,
    )
    ledger = EpistemicLedgerState(history=[])
    frontier = resolve_current_node(workflow, ledger)
    assert len(frontier) == 1
    assert frontier[0].description == "A"


def test_resolve_current_node_empty_graph() -> None:
    topology = DAGTopologyManifest(max_depth=5, edges=[], nodes={}, max_fan_out=5, lifecycle_phase="draft")
    workflow = WorkflowManifest(
        manifest_version="1.0.0",
        genesis_provenance=EpistemicProvenanceReceipt(extracted_by="did:coreason:sys", source_event_id="dummy"),
        topology=topology,
    )
    ledger = EpistemicLedgerState(history=[])
    frontier = resolve_current_node(workflow, ledger)
    assert frontier == []


def test_resolve_current_node_cyclic_non_agent() -> None:
    node_a = SystemNodeProfile(description="A", architectural_intent=".", justification=".", type="system")
    topology = DAGTopologyManifest(
        max_depth=5,
        edges=[("did:coreason:node:a", "did:coreason:node:a")],  # Cycle
        nodes={"did:coreason:node:a": node_a},
        max_fan_out=5,
        lifecycle_phase="draft",
    )
    workflow = WorkflowManifest(
        manifest_version="1.0.0",
        topology=topology,
        genesis_provenance=EpistemicProvenanceReceipt(extracted_by="did:coreason:sys", source_event_id="dummy"),
    )
    ledger = EpistemicLedgerState(history=[])
    frontier = resolve_current_node(workflow, ledger)
    assert frontier == []
