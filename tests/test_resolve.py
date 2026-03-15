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
    assert current_node is node_a


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
    assert current_node is node_b


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
    # Returns None as it doesn't have nodes field handled correctly yet
    assert resolve_current_node(workflow, ledger) is None


def test_resolve_current_node_dag_no_nodes() -> None:
    workflow = get_mock_workflow({}, [])
    ledger = EpistemicLedgerState(history=[])
    assert resolve_current_node(workflow, ledger) is None


def test_resolve_current_node_dag_cyclic() -> None:
    node_a = AgentNodeProfile(description="A", architectural_intent=".", justification=".", type="agent")
    node_b = AgentNodeProfile(description="B", architectural_intent=".", justification=".", type="agent")
    nodes = {"did:coreason:node:a": node_a, "did:coreason:node:b": node_b}
    edges = [("did:coreason:node:a", "did:coreason:node:b"), ("did:coreason:node:b", "did:coreason:node:a")]
    workflow = get_mock_workflow(nodes, edges)
    ledger = EpistemicLedgerState(history=[])
    # Cyclic picks first deterministically
    assert resolve_current_node(workflow, ledger) is node_a


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
    assert resolve_current_node(workflow, ledger) is None


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
    assert resolve_current_node(workflow, ledger) is None


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
    assert resolve_current_node(workflow, ledger) is None


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
    assert resolve_current_node(workflow, ledger) is node_a


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
    assert resolve_current_node(workflow, ledger) is None
