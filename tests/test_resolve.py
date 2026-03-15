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
    AgentNodeProfile,
    DAGTopologyManifest,
    EpistemicLedgerState,
    EpistemicProvenanceReceipt,
    ObservationEvent,
    QuorumPolicy,
    WorkflowManifest,
)

from coreason_orchestrator.resolve import resolve_current_node


def get_mock_workflow(nodes_dict: dict[Any, Any], edges_list: list[tuple[Any, Any]]) -> WorkflowManifest:
    topology = DAGTopologyManifest(
        nodes=nodes_dict, edges=edges_list, max_depth=5, max_fan_out=5, lifecycle_phase="draft"
    )
    return WorkflowManifest(
        genesis_provenance=EpistemicProvenanceReceipt(
            extracted_by="did:coreason:system",
            source_event_id="genesis123",
        ),
        manifest_version="1.0.0",
        topology=topology,
    )


def test_resolve_current_node_empty_ledger() -> None:
    node_a = AgentNodeProfile(description="A test agent", architectural_intent=".", justification=".", type="agent")
    node_b = AgentNodeProfile(description="B test agent", architectural_intent=".", justification=".", type="agent")
    nodes = {"did:coreason:node:a": node_a, "did:coreason:node:b": node_b}
    edges = [("did:coreason:node:a", "did:coreason:node:b")]

    workflow = get_mock_workflow(nodes, edges)
    ledger = EpistemicLedgerState(history=[])

    current_node = resolve_current_node(workflow, ledger)
    assert current_node == [node_a]


def test_resolve_current_node_with_history() -> None:
    node_a = AgentNodeProfile(description="A test agent", architectural_intent=".", justification=".", type="agent")
    node_b = AgentNodeProfile(description="B test agent", architectural_intent=".", justification=".", type="agent")
    nodes = {"did:coreason:node:a": node_a, "did:coreason:node:b": node_b}
    edges = [("did:coreason:node:a", "did:coreason:node:b")]

    workflow = get_mock_workflow(nodes, edges)

    # Node A is completed
    event = ObservationEvent(
        event_id="e1",
        timestamp=1.0,
        type="observation",
        payload={"result": "success"},
        source_node_id="did:coreason:node:a",
        triggering_invocation_id="req1",
    )

    ledger = EpistemicLedgerState(history=[event])

    current_node = resolve_current_node(workflow, ledger)
    assert current_node == [node_b]


def test_resolve_current_node_non_dag() -> None:
    from coreason_manifest.spec.ontology import ConsensusFederationTopologyManifest

    topology = ConsensusFederationTopologyManifest(
        adjudicator_id="did:coreason:node:adj",
        type="macro_federation",
        participant_ids=["did:coreason:node:a", "did:coreason:node:b", "did:coreason:node:c", "did:coreason:node:d"],
        quorum_rules=QuorumPolicy(
            max_tolerable_faults=1,
            min_quorum_size=4,
            state_validation_metric="ledger_hash",
            byzantine_action="quarantine",
        ),
    )
    workflow = WorkflowManifest(
        genesis_provenance=EpistemicProvenanceReceipt(
            extracted_by="did:coreason:system",
            source_event_id="genesis123",
        ),
        manifest_version="1.0.0",
        topology=topology,
    )
    ledger = EpistemicLedgerState(history=[])
    # Returns empty list as it doesn't have nodes field handled correctly yet
    assert resolve_current_node(workflow, ledger) == []


def test_resolve_current_node_dag_no_nodes() -> None:
    workflow = get_mock_workflow({}, [])
    ledger = EpistemicLedgerState(history=[])
    assert resolve_current_node(workflow, ledger) == []


def test_resolve_current_node_dag_cyclic() -> None:
    node_a = AgentNodeProfile(description="A", architectural_intent=".", justification=".", type="agent")
    node_b = AgentNodeProfile(description="B", architectural_intent=".", justification=".", type="agent")
    nodes = {"did:coreason:node:a": node_a, "did:coreason:node:b": node_b}
    edges = [("did:coreason:node:a", "did:coreason:node:b"), ("did:coreason:node:b", "did:coreason:node:a")]
    workflow = get_mock_workflow(nodes, edges)
    ledger = EpistemicLedgerState(history=[])
    # Cyclic picks first deterministically based on sorting keys
    assert resolve_current_node(workflow, ledger) == [node_a]


def test_resolve_current_node_all_completed() -> None:
    node_a = AgentNodeProfile(description="A", architectural_intent=".", justification=".", type="agent")
    nodes = {"did:coreason:node:a": node_a}
    edges: list[tuple[Any, Any]] = []
    workflow = get_mock_workflow(nodes, edges)
    event = ObservationEvent(
        event_id="e1",
        timestamp=1.0,
        type="observation",
        payload={"result": "success"},
        source_node_id="did:coreason:node:a",
        triggering_invocation_id="req1",
    )
    ledger = EpistemicLedgerState(history=[event])
    assert resolve_current_node(workflow, ledger) == []


def test_resolve_current_node_not_agent_profile() -> None:
    from coreason_manifest.spec.ontology import HumanNodeProfile

    node_a = HumanNodeProfile(
        description="Human",
        architectural_intent=".",
        justification=".",
        type="human",
        required_attestation="fido2_webauthn",
    )
    nodes = {"did:coreason:node:a": node_a}
    edges: list[tuple[Any, Any]] = []
    workflow = get_mock_workflow(nodes, edges)
    ledger = EpistemicLedgerState(history=[])
    assert resolve_current_node(workflow, ledger) == []


def test_resolve_current_node_cyclic_not_agent_profile() -> None:
    from coreason_manifest.spec.ontology import HumanNodeProfile

    node_a = HumanNodeProfile(
        description="Human",
        architectural_intent=".",
        justification=".",
        type="human",
        required_attestation="fido2_webauthn",
    )
    nodes = {"did:coreason:node:a": node_a}
    edges = [("did:coreason:node:a", "did:coreason:node:a")]
    workflow = get_mock_workflow(nodes, edges)
    ledger = EpistemicLedgerState(history=[])
    assert resolve_current_node(workflow, ledger) == []


def test_resolve_current_node_non_dag_with_nodes() -> None:
    from coreason_manifest.spec.ontology import SwarmTopologyManifest

    node_a = AgentNodeProfile(description="A", architectural_intent=".", justification=".", type="agent")
    topology = SwarmTopologyManifest(
        nodes={"did:coreason:node:a": node_a},
        lifecycle_phase="draft",
        architectural_intent="test",
        justification="test",
        type="swarm",
    )
    workflow = WorkflowManifest(
        genesis_provenance=EpistemicProvenanceReceipt(
            extracted_by="did:coreason:system",
            source_event_id="genesis123",
        ),
        manifest_version="1.0.0",
        topology=topology,
    )
    ledger = EpistemicLedgerState(history=[])
    assert resolve_current_node(workflow, ledger) == [node_a]


def test_resolve_current_node_non_dag_not_agent() -> None:
    from coreason_manifest.spec.ontology import HumanNodeProfile, SwarmTopologyManifest

    node_a = HumanNodeProfile(
        description="Human",
        architectural_intent=".",
        justification=".",
        type="human",
        required_attestation="fido2_webauthn",
    )
    topology = SwarmTopologyManifest(
        nodes={"did:coreason:node:a": node_a},
        lifecycle_phase="draft",
        architectural_intent="test",
        justification="test",
        type="swarm",
    )
    workflow = WorkflowManifest(
        genesis_provenance=EpistemicProvenanceReceipt(
            extracted_by="did:coreason:system",
            source_event_id="genesis123",
        ),
        manifest_version="1.0.0",
        topology=topology,
    )
    ledger = EpistemicLedgerState(history=[])
    assert resolve_current_node(workflow, ledger) == []


def test_resolve_current_node_multiple_concurrent_roots() -> None:
    node_a = AgentNodeProfile(description="A", architectural_intent=".", justification=".", type="agent")
    node_b = AgentNodeProfile(description="B", architectural_intent=".", justification=".", type="agent")
    node_c = AgentNodeProfile(description="C", architectural_intent=".", justification=".", type="agent")

    nodes = {
        "did:coreason:node:a": node_a,
        "did:coreason:node:b": node_b,
        "did:coreason:node:c": node_c,
    }

    # A and B have no incoming edges. C depends on A and B.
    edges = [
        ("did:coreason:node:a", "did:coreason:node:c"),
        ("did:coreason:node:b", "did:coreason:node:c"),
    ]

    workflow = get_mock_workflow(nodes, edges)
    ledger = EpistemicLedgerState(history=[])

    # Initial state should resolve both roots
    current_nodes = resolve_current_node(workflow, ledger)
    assert current_nodes == [node_a, node_b]


def test_resolve_current_node_bypassed_steps() -> None:
    """Verify accumulation of bypassed_steps preventing execution of bypassed nodes."""
    node_a = AgentNodeProfile(description="A", architectural_intent=".", justification=".", type="agent")
    node_b = AgentNodeProfile(description="B", architectural_intent=".", justification=".", type="agent")
    node_c = AgentNodeProfile(description="C", architectural_intent=".", justification=".", type="agent")

    nodes = {
        "did:coreason:node:a": node_a,
        "did:coreason:node:b": node_b,
        "did:coreason:node:c": node_c,
    }
    # A -> B -> C
    edges = [("did:coreason:node:a", "did:coreason:node:b"), ("did:coreason:node:b", "did:coreason:node:c")]
    workflow = get_mock_workflow(nodes, edges)

    # A completes, and outputs an ObservationEvent containing a DynamicRoutingManifest dict that bypasses B
    obs_a = ObservationEvent(
        event_id="e1",
        timestamp=1.0,
        type="observation",
        payload={
            "result": "ok",
            "active_subgraphs": {},
            "bypassed_steps": [
                {
                    "artifact_event_id": "e1",
                    "bypassed_node_id": "did:coreason:node:b",
                    "justification": "modality_mismatch",
                    "cryptographic_null_hash": "a" * 64,
                }
            ],
        },
        source_node_id="did:coreason:node:a",
    )
    ledger = EpistemicLedgerState(history=[obs_a])
    frontier = resolve_current_node(workflow, ledger)

    # B is bypassed, so it is considered completed. Predecessors of C are B (completed), so C should be the frontier.
    assert len(frontier) == 1
    assert frontier[0] == node_c


def test_resolve_current_node_active_subgraphs() -> None:
    """Verify frontier filtering using active_subgraphs."""
    node_a = AgentNodeProfile(description="A", architectural_intent=".", justification=".", type="agent")
    node_b = AgentNodeProfile(description="B", architectural_intent=".", justification=".", type="agent")
    node_c = AgentNodeProfile(description="C", architectural_intent=".", justification=".", type="agent")

    nodes = {
        "did:coreason:node:a": node_a,
        "did:coreason:node:b": node_b,
        "did:coreason:node:c": node_c,
    }
    # A -> B, A -> C
    edges = [("did:coreason:node:a", "did:coreason:node:b"), ("did:coreason:node:a", "did:coreason:node:c")]
    workflow = get_mock_workflow(nodes, edges)

    # A completes, and its ObservationEvent payload specifies only C is active
    obs_a = ObservationEvent(
        event_id="e1",
        timestamp=1.0,
        type="observation",
        payload={"result": "ok", "active_subgraphs": {"text": ["did:coreason:node:c"]}, "bypassed_steps": []},
        source_node_id="did:coreason:node:a",
    )
    ledger = EpistemicLedgerState(history=[obs_a])
    frontier = resolve_current_node(workflow, ledger)

    # Normally B and C would be active, but only C is in active_subgraphs.
    assert len(frontier) == 1
    assert frontier[0] == node_c


def test_resolve_current_node_multiple_routing_manifests() -> None:
    """Verify handling of multiple DynamicRoutingManifest payloads in Observations."""
    node_a = AgentNodeProfile(description="A", architectural_intent=".", justification=".", type="agent")
    node_b = AgentNodeProfile(description="B", architectural_intent=".", justification=".", type="agent")
    node_c = AgentNodeProfile(description="C", architectural_intent=".", justification=".", type="agent")
    node_d = AgentNodeProfile(description="D", architectural_intent=".", justification=".", type="agent")

    nodes = {
        "did:coreason:node:a": node_a,
        "did:coreason:node:b": node_b,
        "did:coreason:node:c": node_c,
        "did:coreason:node:d": node_d,
    }
    # A -> B, A -> C, B -> D, C -> D
    edges = [
        ("did:coreason:node:a", "did:coreason:node:b"),
        ("did:coreason:node:a", "did:coreason:node:c"),
        ("did:coreason:node:b", "did:coreason:node:d"),
        ("did:coreason:node:c", "did:coreason:node:d"),
    ]
    workflow = get_mock_workflow(nodes, edges)

    obs_a = ObservationEvent(
        event_id="e1",
        timestamp=1.0,
        type="observation",
        payload={
            "result": "ok",
            "active_subgraphs": {"text": ["did:coreason:node:b", "did:coreason:node:c"]},
            "bypassed_steps": [],
        },
        source_node_id="did:coreason:node:a",
    )
    obs_b = ObservationEvent(
        event_id="e2",
        timestamp=3.0,
        type="observation",
        payload={
            "result": "ok",
            "active_subgraphs": {"text": ["did:coreason:node:b", "did:coreason:node:c", "did:coreason:node:d"]},
            "bypassed_steps": [
                {
                    "artifact_event_id": "e2",
                    "bypassed_node_id": "did:coreason:node:c",
                    "justification": "modality_mismatch",
                    "cryptographic_null_hash": "b" * 64,
                }
            ],
        },
        source_node_id="did:coreason:node:b",
    )
    ledger = EpistemicLedgerState(history=[obs_a, obs_b])

    frontier = resolve_current_node(workflow, ledger)

    # A is complete. B is complete. C is bypassed (so complete). D requires B and C complete, both are.
    # So D should be in the frontier. D is in active_subgraphs.
    assert len(frontier) == 1
    assert frontier[0] == node_d
