# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator

from unittest.mock import AsyncMock

import pytest
from coreason_manifest.spec.ontology import (
    AgentAttestationReceipt,
    AgentNodeProfile,
    DAGTopologyManifest,
    EpistemicLedgerState,
    EpistemicProvenanceReceipt,
    ObservationEvent,
    PermissionBoundaryPolicy,
    SideEffectProfile,
    TokenBurnReceipt,
    ToolInvocationEvent,
    ToolManifest,
    WorkflowManifest,
    ZeroKnowledgeReceipt,
)

from coreason_orchestrator.engine import CoreOrchestrator
from coreason_orchestrator.interfaces import InferenceConvergenceError


def get_mock_workflow() -> WorkflowManifest:
    topology = DAGTopologyManifest(nodes={}, edges=[], max_depth=5, max_fan_out=5, lifecycle_phase="draft")
    return WorkflowManifest(
        genesis_provenance=EpistemicProvenanceReceipt(
            extracted_by="did:coreason:system",
            source_event_id="genesis123",
        ),
        manifest_version="1.0.0",
        topology=topology,
    )


def get_mock_node() -> AgentNodeProfile:
    return AgentNodeProfile(
        description="A test agent",
        architectural_intent="To test.",
        justification="Unit tests.",
        type="agent",
    )


@pytest.mark.asyncio
async def test_delegate_to_cognitive_plane_success() -> None:
    """Verifies that the orchestrator properly delegates to the inference engine and appends to the ledger."""
    workflow = get_mock_workflow()
    ledger = EpistemicLedgerState(history=[])

    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()

    # Create mock intent/event and token receipt
    mock_event = ObservationEvent(event_id="e1", timestamp=1.0, type="observation", payload={"result": "success"})
    mock_burn = TokenBurnReceipt(
        event_id="e2",
        timestamp=2.0,
        type="token_burn",
        tool_invocation_id="req1",
        input_tokens=10,
        output_tokens=20,
        burn_magnitude=30,
    )

    inference_engine.generate_intent.return_value = (mock_event, mock_burn, None)

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
    )

    node = get_mock_node()

    # Execute
    await orchestrator.delegate_to_cognitive_plane(node)

    # Verify calls
    inference_engine.generate_intent.assert_called_once_with(node, ledger)

    # Verify ledger was updated correctly
    # Note: append_event nullifies event_id to calculate a hash, so the ids will differ from e1/e2
    assert len(orchestrator.ledger.history) == 2
    assert isinstance(orchestrator.ledger.history[0], ObservationEvent)
    assert orchestrator.ledger.history[0].payload == {"result": "success"}

    assert isinstance(orchestrator.ledger.history[1], TokenBurnReceipt)
    assert orchestrator.ledger.history[1].input_tokens == 10
    assert orchestrator.ledger.history[1].burn_magnitude == 30


@pytest.mark.asyncio
async def test_delegate_to_cognitive_plane_inference_error() -> None:
    """Verifies that an exception from the inference engine propagates correctly."""
    workflow = get_mock_workflow()
    ledger = EpistemicLedgerState(history=[])

    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()

    inference_engine.generate_intent.side_effect = InferenceConvergenceError("Failed to converge")

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
    )

    node = get_mock_node()

    with pytest.raises(InferenceConvergenceError, match="Failed to converge"):
        await orchestrator.delegate_to_cognitive_plane(node)

    # Ledger should remain unchanged
    assert len(orchestrator.ledger.history) == 0


@pytest.mark.asyncio
async def test_delegate_to_kinetic_plane_success() -> None:
    """Verifies that the orchestrator properly delegates and synthesizes the ObservationEvent."""
    workflow = get_mock_workflow()
    ledger = EpistemicLedgerState(history=[])

    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()

    # The actuator should strictly return a raw JsonPrimitiveState, NOT an ObservationEvent
    mock_payload = {"status": "executed", "data": "test_data"}
    actuator_engine.execute.return_value = mock_payload

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
    )

    # Mock tool intent and manifest
    intent = ToolInvocationEvent(
        event_id="tool_inv_123",
        timestamp=1.0,
        type="tool_invocation",
        tool_name="test_tool",
        parameters={"param": "value"},
        agent_attestation=AgentAttestationReceipt(
            training_lineage_hash="a" * 64,
            developer_signature="sig1",
            capability_merkle_root="b" * 64,
            credential_presentations=[],
        ),
        zk_proof=ZeroKnowledgeReceipt(
            proof_protocol="zk-SNARK",
            public_inputs_hash="c" * 64,
            verifier_key_id="key1",
            cryptographic_blob="blob1",
            latent_state_commitments={},
        ),
    )

    manifest = ToolManifest(
        tool_name="test_tool",
        description="A test tool",
        input_schema={"type": "object", "properties": {"param": {"type": "string"}}, "required": ["param"]},
        side_effects=SideEffectProfile(is_idempotent=True, mutates_state=False),
        permissions=PermissionBoundaryPolicy(network_access=False, file_system_mutation_forbidden=True),
    )

    # Execute
    await orchestrator.delegate_to_kinetic_plane(intent, manifest)

    # Verify calls
    actuator_engine.execute.assert_called_once()

    call_args = actuator_engine.execute.call_args[0]
    assert call_args[0] == intent
    assert call_args[1] == manifest
    assert call_args[2].max_retained_tokens == 5000  # workflow.topology.max_fan_out * 1000

    # Verify ledger was updated correctly
    assert len(orchestrator.ledger.history) == 1
    observation = orchestrator.ledger.history[0]

    assert isinstance(observation, ObservationEvent)
    assert observation.type == "observation"
    assert observation.payload == mock_payload
    # Crucially, verify that the orchestrator mapped the triggering_invocation_id securely
    assert observation.triggering_invocation_id == "tool_inv_123"


@pytest.mark.asyncio
async def test_delegate_to_kinetic_plane_primitive_payload() -> None:
    """Verifies that the orchestrator properly wraps non-dict raw payload into a dictionary."""
    workflow = get_mock_workflow()
    ledger = EpistemicLedgerState(history=[])

    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()

    # Return a primitive like a string or int
    actuator_engine.execute.return_value = "primitive_success"

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
    )

    intent = ToolInvocationEvent(
        event_id="tool_inv_456",
        timestamp=1.0,
        type="tool_invocation",
        tool_name="test_tool",
        parameters={},
        agent_attestation=AgentAttestationReceipt(
            training_lineage_hash="a" * 64,
            developer_signature="sig1",
            capability_merkle_root="b" * 64,
            credential_presentations=[],
        ),
        zk_proof=ZeroKnowledgeReceipt(
            proof_protocol="zk-SNARK",
            public_inputs_hash="c" * 64,
            verifier_key_id="key1",
            cryptographic_blob="blob1",
            latent_state_commitments={},
        ),
    )

    manifest = ToolManifest(
        tool_name="test_tool",
        description="A test tool",
        input_schema={"type": "object", "properties": {}, "required": []},
        side_effects=SideEffectProfile(is_idempotent=True, mutates_state=False),
        permissions=PermissionBoundaryPolicy(network_access=False, file_system_mutation_forbidden=True),
    )

    await orchestrator.delegate_to_kinetic_plane(intent, manifest)

    assert len(orchestrator.ledger.history) == 1
    observation = orchestrator.ledger.history[0]

    assert isinstance(observation, ObservationEvent)
    assert observation.payload == {"result": "primitive_success"}
    assert observation.triggering_invocation_id == "tool_inv_456"
