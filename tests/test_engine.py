import asyncio
from unittest.mock import AsyncMock, patch

# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator
import pytest
from coreason_manifest.spec.ontology import (
    ActionSpaceManifest,
    AgentAttestationReceipt,
    AgentNodeProfile,
    BargeInInterruptEvent,
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

    inference_engine.generate_intent.return_value = (mock_event, mock_burn, None, None)

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
    )

    node = get_mock_node()
    mock_action_space = ActionSpaceManifest(action_space_id="test_space", native_tools=[])

    # Execute
    await orchestrator.delegate_to_cognitive_plane(node, "node_1", mock_action_space)

    # Verify calls
    inference_engine.generate_intent.assert_called_once_with(node, ledger, "node_1", mock_action_space)

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
    mock_action_space = ActionSpaceManifest(action_space_id="test_space", native_tools=[])

    with pytest.raises(InferenceConvergenceError, match="Failed to converge"):
        await orchestrator.delegate_to_cognitive_plane(node, "node_1", mock_action_space)

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
        event_id="1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        timestamp=1.0,
        type="tool_invocation",
        tool_name="test_tool",
        parameters={"test_param": "test_value"},
        authorized_budget_magnitude=1,
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
    assert call_args[2] is None  # Default when eviction policy is absent

    # Test dynamic eviction policy propagation
    from coreason_manifest.spec.ontology import EvictionPolicy

    policy = EvictionPolicy(strategy="fifo", max_retained_tokens=10000, protected_event_ids=[])
    orchestrator.ledger = orchestrator.ledger.model_copy(update={"eviction_policy": policy})
    await orchestrator.delegate_to_kinetic_plane(intent, manifest)
    call_args = actuator_engine.execute.call_args[0]
    assert call_args[2] is policy

    # Test dynamic max_retained_tokens overriding via GovernancePolicy (for coverage of hydration fallback)
    from coreason_manifest.spec.ontology import ConstitutionalPolicy, GlobalGovernancePolicy

    orchestrator.ledger = orchestrator.ledger.model_copy(update={"eviction_policy": None})
    orchestrator.workflow = orchestrator.workflow.model_copy(
        update={
            "governance": GlobalGovernancePolicy(
                mandatory_license_rule=ConstitutionalPolicy(
                    rule_id="PPL_3_0_COMPLIANCE", description="License Check", severity="critical", forbidden_intents=[]
                ),
                max_budget_magnitude=8000,
                max_global_tokens=1000,
                global_timeout_seconds=100,
            )
        }
    )
    await orchestrator.delegate_to_kinetic_plane(intent, manifest)

    # Clean up overriding
    orchestrator.ledger = orchestrator.ledger.model_copy(update={"eviction_policy": None})

    # Verify ledger was updated correctly
    assert len(orchestrator.ledger.history) == 3
    observation = orchestrator.ledger.history[-1]

    assert isinstance(observation, ObservationEvent)
    assert observation.type == "observation"
    assert observation.payload == mock_payload
    # Crucially, verify that the orchestrator mapped the triggering_invocation_id securely
    assert observation.triggering_invocation_id == "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"


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
        parameters={"dummy": "dummy"},
        authorized_budget_magnitude=1,
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


@pytest.mark.asyncio
async def test_handle_preemption() -> None:
    """Verifies that preemption intercepts correctly map to discarding."""
    workflow = get_mock_workflow()
    ledger = EpistemicLedgerState(history=[])
    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
    )

    barge_in_event = BargeInInterruptEvent(
        event_id="initial_barge_in",
        timestamp=1.0,
        type="barge_in",
        target_event_id="none_yet",
        sensory_trigger=None,
        retained_partial_payload=None,
        epistemic_disposition="retain_as_context",
    )

    orchestrator.handle_preemption(barge_in_event, "tool_inv_running_123")

    assert len(orchestrator.ledger.history) == 1
    terminal_event = orchestrator.ledger.history[0]

    assert isinstance(terminal_event, BargeInInterruptEvent)
    assert terminal_event.target_event_id == "tool_inv_running_123"
    assert terminal_event.epistemic_disposition == "discard"


@pytest.mark.asyncio
async def test_slash_byzantine_fault() -> None:
    """Verifies that a byzantine fault appends a token burn receipt."""
    workflow = get_mock_workflow()
    ledger = EpistemicLedgerState(history=[])
    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
    )

    orchestrator.slash_byzantine_fault("did:coreason:agent:evil_node", "bad_invocation_456", 500)

    assert len(orchestrator.ledger.history) == 2
    observation = orchestrator.ledger.history[0]
    assert isinstance(observation, ObservationEvent)
    assert observation.source_node_id == "did:coreason:agent:evil_node"
    assert observation.payload["burn_magnitude"] == 500

    burn_receipt = orchestrator.ledger.history[1]

    assert isinstance(burn_receipt, TokenBurnReceipt)
    assert burn_receipt.tool_invocation_id == "bad_invocation_456"
    assert burn_receipt.burn_magnitude == 500
    assert burn_receipt.input_tokens == 0
    assert burn_receipt.output_tokens == 0


@pytest.mark.asyncio
async def test_tick_terminal() -> None:
    """Verifies tick returns False when graph is terminal."""
    workflow = get_mock_workflow()
    ledger = EpistemicLedgerState(history=[])

    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
    )

    # workflow has empty nodes, so terminal
    result = await orchestrator.tick()
    # Print debug info
    print(f"Ledger history after tick: {[e.type for e in orchestrator.ledger.history]}")
    assert result is False
    inference_engine.generate_intent.assert_not_called()
    actuator_engine.execute.assert_not_called()


@pytest.mark.asyncio
async def test_tick_cognitive_delegation() -> None:
    """Verifies tick delegates to cognitive plane when needed."""
    node_a = AgentNodeProfile(
        description="A", architectural_intent=".", justification=".", type="agent", action_space_id="test_space"
    )

    # Needs to copy safely
    workflow = get_mock_workflow()
    new_topo = workflow.topology.model_copy(update={"nodes": {"did:coreason:node:a": node_a}})
    workflow = workflow.model_copy(update={"topology": new_topo})

    ledger = EpistemicLedgerState(history=[])

    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()

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
    inference_engine.generate_intent.return_value = (mock_event, mock_burn, None, None)

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
        action_space_registry={"test_space": ActionSpaceManifest(action_space_id="test_space", native_tools=[])},
    )

    result = await orchestrator.tick()
    # Print debug info
    print(f"Ledger history after tick: {[e.type for e in orchestrator.ledger.history]}")
    assert result is True
    inference_engine.generate_intent.assert_called_once_with(
        node_a, ledger, "did:coreason:node:a", ActionSpaceManifest(action_space_id="test_space", native_tools=[])
    )


@pytest.mark.asyncio
async def test_tick_kinetic_delegation() -> None:
    """Verifies tick delegates to kinetic plane when latest event is ToolInvocationEvent."""
    node_a = AgentNodeProfile(
        description="A", architectural_intent=".", justification=".", type="agent", action_space_id="test_space"
    )
    workflow = get_mock_workflow()
    new_topo = workflow.topology.model_copy(update={"nodes": {"did:coreason:node:a": node_a}})
    workflow = workflow.model_copy(update={"topology": new_topo})

    intent = ToolInvocationEvent(
        event_id="1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        timestamp=1.0,
        type="tool_invocation",
        tool_name="test_tool",
        parameters={"dummy": "dummy"},
        authorized_budget_magnitude=1,
        agent_attestation=AgentAttestationReceipt(
            training_lineage_hash="a" * 64,
            developer_signature="sig",
            capability_merkle_root="b" * 64,
            credential_presentations=[],
        ),
        zk_proof=ZeroKnowledgeReceipt(
            proof_protocol="zk-SNARK",
            public_inputs_hash="c" * 64,
            verifier_key_id="key1",
            cryptographic_blob="blob1",
        ),
    )
    ledger = EpistemicLedgerState(history=[intent])

    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()
    actuator_engine.execute.return_value = {"status": "ok"}

    manifest = ToolManifest(
        tool_name="test_tool",
        description="A test tool",
        input_schema={"type": "object", "properties": {}, "required": []},
        side_effects=SideEffectProfile(is_idempotent=True, mutates_state=False),
        permissions=PermissionBoundaryPolicy(network_access=False, file_system_mutation_forbidden=True),
    )
    action_space = ActionSpaceManifest(action_space_id="test_space", native_tools=[manifest])
    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
        action_space_registry={"test_space": action_space},
    )

    result = await orchestrator.tick()
    # Print debug info
    print(f"Ledger history after tick: {[e.type for e in orchestrator.ledger.history]}")
    assert result is True
    actuator_engine.execute.assert_called_once()

    # Should append observation event
    assert len(orchestrator.ledger.history) == 2
    assert isinstance(orchestrator.ledger.history[-1], ObservationEvent)


@pytest.mark.asyncio
async def test_run_event_loop_success() -> None:
    """Verifies event loop runs and terminates correctly."""
    workflow = get_mock_workflow()
    ledger = EpistemicLedgerState(history=[])

    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
    )

    # Empty nodes = terminal, so tick() returns False immediately
    final_ledger = await orchestrator.run_event_loop()

    inference_engine.generate_intent.assert_not_called()
    assert isinstance(final_ledger, EpistemicLedgerState)
    assert final_ledger is orchestrator.ledger


@pytest.mark.asyncio
async def test_run_event_loop_exception(capsys: pytest.CaptureFixture[str]) -> None:
    """Verifies event loop handles exceptions and dumps state."""
    workflow = get_mock_workflow()
    ledger = EpistemicLedgerState(history=[])

    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
    )

    with (
        patch.object(orchestrator, "tick", new_callable=AsyncMock, side_effect=[True, ValueError("Tick failed")]),
        pytest.raises(Exception, match=r"Tick failed"),
    ):
        await orchestrator.run_event_loop()

    # Verify that the state was dumped to stdout
    captured = capsys.readouterr()
    assert ledger.model_dump_json() in captured.out


@pytest.mark.asyncio
async def test_run_event_loop_preemption() -> None:
    """Verifies that the event loop safely handles preemption and cascade cancellation."""
    workflow = get_mock_workflow()
    ledger = EpistemicLedgerState(history=[])

    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
    )

    barge_in_event = BargeInInterruptEvent(
        event_id="barge_in_1",
        timestamp=1.0,
        type="barge_in",
        target_event_id="none",
        sensory_trigger=None,
        retained_partial_payload=None,
        epistemic_disposition="retain_as_context",
    )

    # To trigger preemption, we will mock tick to just block (sleep) so the loop stays alive
    # And we'll put an item in the queue.
    async def mock_tick() -> bool:
        await asyncio.sleep(0.1)
        return True

    orchestrator.tick = mock_tick  # type: ignore

    orchestrator.interrupt_queue.put_nowait((barge_in_event, "active_tool_inv"))

    # When we run the event loop, it should process the interrupt, cancel the tick, and gracefully exit
    final_ledger = await asyncio.wait_for(orchestrator.run_event_loop(), timeout=2.0)

    # Verify that preemption handled it correctly
    assert len(orchestrator.ledger.history) == 1
    assert isinstance(final_ledger, EpistemicLedgerState)
    assert final_ledger is orchestrator.ledger
    terminal_event = orchestrator.ledger.history[0]
    assert isinstance(terminal_event, BargeInInterruptEvent)
    assert terminal_event.target_event_id == "active_tool_inv"
    assert terminal_event.epistemic_disposition == "discard"


@pytest.mark.asyncio
async def test_run_event_loop_general_exception(capsys: pytest.CaptureFixture[str]) -> None:
    """Verifies event loop handles exceptions and dumps state for general exceptions."""
    workflow = get_mock_workflow()
    ledger = EpistemicLedgerState(history=[])

    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
    )

    with (
        patch.object(orchestrator, "tick", new_callable=AsyncMock, side_effect=ValueError("General exception")),
        pytest.raises(Exception, match="General exception"),
    ):
        await orchestrator.run_event_loop()

    # Verify that the state was dumped to stdout
    captured = capsys.readouterr()
    assert ledger.model_dump_json() in captured.out


@pytest.mark.asyncio
async def test_tick_cognitive_plane_fault() -> None:
    """Verifies that Cognitive Plane faults are properly gathered into an ExceptionGroup."""
    workflow = get_mock_workflow()
    ledger = EpistemicLedgerState(history=[])

    inference_engine = AsyncMock()
    inference_engine.generate_intent.side_effect = RuntimeError("Cognitive failure")
    actuator_engine = AsyncMock()

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
    )

    # Node to trigger delegate_to_cognitive_plane
    node_a = AgentNodeProfile(description="A", architectural_intent=".", justification=".", type="agent")
    new_topo = workflow.topology.model_copy(update={"nodes": {"did:coreason:node:a": node_a}})
    workflow = workflow.model_copy(update={"topology": new_topo})
    orchestrator.workflow = workflow

    with pytest.raises(ExceptionGroup) as exc_info:
        await orchestrator.tick()
    # Print debug info
    print(f"Ledger history after tick: {[e.type for e in orchestrator.ledger.history]}")
    assert "Cognitive Plane Faults" in str(exc_info.value)
    assert len(exc_info.value.exceptions) == 1


@pytest.mark.asyncio
async def test_tick_kinetic_delegation_tool_not_found() -> None:
    """Verifies that an exception is raised when tool is missing from ActionSpaceManifest."""
    node_a = AgentNodeProfile(
        description="A", architectural_intent=".", justification=".", type="agent", action_space_id="test_space"
    )
    workflow = get_mock_workflow()
    new_topo = workflow.topology.model_copy(update={"nodes": {"did:coreason:node:a": node_a}})
    workflow = workflow.model_copy(update={"topology": new_topo})

    intent = ToolInvocationEvent(
        event_id="1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        timestamp=1.0,
        type="tool_invocation",
        tool_name="missing_tool",
        parameters={"dummy": "dummy"},
        authorized_budget_magnitude=1,
        agent_attestation=AgentAttestationReceipt(
            training_lineage_hash="a" * 64,
            developer_signature="sig",
            capability_merkle_root="b" * 64,
            credential_presentations=[],
        ),
        zk_proof=ZeroKnowledgeReceipt(
            proof_protocol="zk-SNARK",
            public_inputs_hash="c" * 64,
            verifier_key_id="key1",
            cryptographic_blob="blob1",
        ),
    )
    ledger = EpistemicLedgerState(history=[intent])

    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()
    actuator_engine.execute.return_value = {"status": "ok"}

    action_space = ActionSpaceManifest(action_space_id="test_space", native_tools=[])
    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
        action_space_registry={"test_space": action_space},
    )

    result = await orchestrator.tick()
    # Print debug info
    print(f"Ledger history after tick: {[getattr(e, 'type', type(e).__name__) for e in orchestrator.ledger.history]}")

    assert result is True
    # Verify a System2RemediationIntent was appended to the ledger
    from coreason_manifest.spec.ontology import System2RemediationIntent

    last_event = orchestrator.ledger.history[-1]
    assert isinstance(last_event, System2RemediationIntent)
    assert last_event.target_node_id == "did:coreason:node:a"
    assert last_event.violation_receipts[0].failing_pointer == "/tool_name"
    assert "missing_tool" in last_event.violation_receipts[0].diagnostic_message


@pytest.mark.asyncio
async def test_tick_kinetic_delegation_general_exception() -> None:
    """Verifies that an exception during kinetic plane dispatch is caught and re-raised."""
    node_a = AgentNodeProfile(
        description="A", architectural_intent=".", justification=".", type="agent", action_space_id="test_space"
    )
    workflow = get_mock_workflow()
    new_topo = workflow.topology.model_copy(update={"nodes": {"did:coreason:node:a": node_a}})
    workflow = workflow.model_copy(update={"topology": new_topo})

    intent = ToolInvocationEvent(
        event_id="1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        timestamp=1.0,
        type="tool_invocation",
        tool_name="existing_tool",
        parameters={"dummy": "dummy"},
        authorized_budget_magnitude=1,
        agent_attestation=AgentAttestationReceipt(
            training_lineage_hash="a" * 64,
            developer_signature="sig",
            capability_merkle_root="b" * 64,
            credential_presentations=[],
        ),
        zk_proof=ZeroKnowledgeReceipt(
            proof_protocol="zk-SNARK",
            public_inputs_hash="c" * 64,
            verifier_key_id="key1",
            cryptographic_blob="blob1",
        ),
    )
    ledger = EpistemicLedgerState(history=[intent])

    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()
    actuator_engine.execute.side_effect = RuntimeError("Kinetic crash")

    action_space = ActionSpaceManifest(
        action_space_id="test_space",
        native_tools=[
            ToolManifest.model_construct(
                tool_name="existing_tool",
                description="test",
                input_schema={"type": "object", "properties": {}},
                side_effects={"impacts_state": False, "mutates_external_systems": False},  # type: ignore[arg-type]
                permissions={"network_access": False, "file_system_access": False, "allowed_domains": []},  # type: ignore[arg-type]
            )
        ],
    )
    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
        action_space_registry={"test_space": action_space},
    )

    with pytest.raises(ExceptionGroup) as exc_info:
        await orchestrator.tick()
    # Print debug info
    print(f"Ledger history after tick: {[e.type for e in orchestrator.ledger.history]}")

    assert "Kinetic crash" in str(exc_info.value.exceptions[0])


def test_engine_zero_cost_macro() -> None:
    from coreason_manifest.spec.ontology import DAGTopologyManifest, EpistemicProvenanceReceipt, WorkflowManifest

    from coreason_orchestrator.engine import CoreOrchestrator

    base_topology = DAGTopologyManifest.model_construct(nodes={}, max_depth=1, max_fan_out=1)

    class MockTopology:
        def compile_to_base_topology(self) -> DAGTopologyManifest:
            return base_topology

    workflow = WorkflowManifest.model_construct(
        genesis_provenance=EpistemicProvenanceReceipt.model_construct(extracted_by="", source_event_id=""),
        manifest_version="1",
        topology=MockTopology(),  # type: ignore[arg-type, call-arg]
    )

    orchestrator = CoreOrchestrator(workflow, None, None, None)  # type: ignore[arg-type]
    assert orchestrator.workflow.topology == base_topology


@pytest.mark.asyncio
async def test_inject_observation() -> None:
    """Verifies inject_observation properly synthensizes and appends to the ledger."""
    workflow = get_mock_workflow()
    ledger = EpistemicLedgerState(history=[])

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=AsyncMock(),
        actuator_engine=AsyncMock(),
    )

    await orchestrator.inject_observation("Test human message")

    assert len(orchestrator.ledger.history) == 1
    assert isinstance(orchestrator.ledger.history[0], ObservationEvent)
    assert orchestrator.ledger.history[0].payload == {"user_input": "Test human message"}


def test_dump_partial_state() -> None:
    """Verifies dump_partial_state returns the current ledger state."""
    workflow = get_mock_workflow()
    ledger = EpistemicLedgerState(history=[])

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=AsyncMock(),
        actuator_engine=AsyncMock(),
    )

    result = orchestrator.dump_partial_state()
    assert result is ledger


@pytest.mark.asyncio
async def test_append_to_ledger() -> None:
    workflow = get_mock_workflow()
    ledger = EpistemicLedgerState(history=[])
    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()
    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
    )
    from coreason_manifest.spec.ontology import ManifestViolationReceipt, System2RemediationIntent

    from coreason_orchestrator.factory import EventFactory

    intent = EventFactory.build_event(
        System2RemediationIntent,
        fault_id="fault-123",
        target_node_id="did:coreason:orchestrator",
        violation_receipts=[
            ManifestViolationReceipt(
                diagnostic_message="test", failing_pointer="/test", violation_type="ontology_mismatch"
            )
        ],
    )

    await orchestrator.append_to_ledger(intent)
    assert len(orchestrator.ledger.history) > 0
    assert orchestrator.ledger.history[-1] == intent


def test_init_without_workflow() -> None:
    """Verifies orchestrator can instantiate without a workflow (None)."""
    ledger = EpistemicLedgerState(history=[])

    orchestrator = CoreOrchestrator(
        workflow=None,  # type: ignore[arg-type]
        ledger=ledger,
        inference_engine=AsyncMock(),
        actuator_engine=AsyncMock(),
    )

    assert orchestrator.workflow is None


@pytest.mark.asyncio
async def test_tick_delegate_cognitive_plane_node_id_resolution() -> None:
    node_a = AgentNodeProfile(description="A", architectural_intent=".", justification=".", type="agent")
    workflow = get_mock_workflow()
    new_topo = workflow.topology.model_copy(update={"nodes": {"did:coreason:node:a": node_a}})
    workflow = workflow.model_copy(update={"topology": new_topo})

    ledger = EpistemicLedgerState(history=[])
    inference_engine = AsyncMock()
    # Mock return value for generate_intent

    mock_event = ObservationEvent.model_construct(event_id="test", timestamp=1.0, type="observation", payload={})
    from coreason_manifest.spec.ontology import TokenBurnReceipt

    mock_burn = TokenBurnReceipt.model_construct(
        event_id="burn",
        timestamp=1.0,
        type="token_burn",
        tool_invocation_id="test",
        input_tokens=0,
        output_tokens=0,
        burn_magnitude=0,
    )
    inference_engine.generate_intent.return_value = (mock_event, mock_burn, None, None)

    actuator_engine = AsyncMock()

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
    )

    result = await orchestrator.tick()
    # Print debug info
    print(f"Ledger history after tick: {[e.type for e in orchestrator.ledger.history]}")
    assert result is True
    # wait for tasks to finish
    import asyncio

    await asyncio.sleep(0.1)

    inference_engine.generate_intent.assert_called_once()
    args = inference_engine.generate_intent.call_args[0]
    assert args[0] is node_a
    assert args[1] is ledger
    assert args[2] == "did:coreason:node:a"
    assert args[3].action_space_id == "default"


@pytest.mark.asyncio
async def test_tick_delegate_cognitive_plane_node_action_space_resolution() -> None:
    node_a = AgentNodeProfile(
        description="A", architectural_intent=".", justification=".", type="agent", action_space_id="custom_space"
    )
    workflow = get_mock_workflow()
    new_topo = workflow.topology.model_copy(update={"nodes": {"did:coreason:node:a": node_a}})
    workflow = workflow.model_copy(update={"topology": new_topo})

    ledger = EpistemicLedgerState(history=[])
    inference_engine = AsyncMock()
    # Mock return value for generate_intent

    mock_event = ObservationEvent.model_construct(event_id="test", timestamp=1.0, type="observation", payload={})
    from coreason_manifest.spec.ontology import TokenBurnReceipt

    mock_burn = TokenBurnReceipt.model_construct(
        event_id="burn",
        timestamp=1.0,
        type="token_burn",
        tool_invocation_id="test",
        input_tokens=0,
        output_tokens=0,
        burn_magnitude=0,
    )
    inference_engine.generate_intent.return_value = (mock_event, mock_burn, None, None)

    actuator_engine = AsyncMock()

    custom_space = ActionSpaceManifest(action_space_id="custom_space", native_tools=[])

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
        action_space_registry={"custom_space": custom_space},
    )

    result = await orchestrator.tick()
    # Print debug info
    print(f"Ledger history after tick: {[e.type for e in orchestrator.ledger.history]}")
    assert result is True
    import asyncio

    await asyncio.sleep(0.1)

    inference_engine.generate_intent.assert_called_once()
    args = inference_engine.generate_intent.call_args[0]
    assert args[3] is custom_space


@pytest.mark.asyncio
async def test_run_event_loop_natural_exit() -> None:
    workflow = get_mock_workflow()
    ledger = EpistemicLedgerState(history=[])
    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
    )

    # Mock tick to return False on the first call to simulate natural exit
    orchestrator.tick = AsyncMock(return_value=False)  # type: ignore

    result_ledger = await orchestrator.run_event_loop()

    assert result_ledger is orchestrator.ledger


@pytest.mark.asyncio
async def test_tick_intervention_policy_halt() -> None:
    """Verifies that a pending tool is intercepted and halted by an InterventionPolicy."""
    from coreason_manifest.spec.ontology import InterventionPolicy, ObservationEvent

    policy = InterventionPolicy(trigger="before_tool_execution", blocking=True)
    node_a = AgentNodeProfile(
        description="A",
        architectural_intent=".",
        justification=".",
        type="agent",
        action_space_id="test_space",
        intervention_policies=[policy],
    )
    workflow = get_mock_workflow()
    new_topo = workflow.topology.model_copy(update={"nodes": {"did:coreason:node:a": node_a}})
    workflow = workflow.model_copy(update={"topology": new_topo})

    intent = ToolInvocationEvent(
        event_id="1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        timestamp=1.0,
        type="tool_invocation",
        tool_name="existing_tool",
        parameters={"test_param": "test_value"},
        authorized_budget_magnitude=1,
        agent_attestation=AgentAttestationReceipt(
            training_lineage_hash="a" * 64,
            developer_signature="sig",
            capability_merkle_root="b" * 64,
            credential_presentations=[],
        ),
        zk_proof=ZeroKnowledgeReceipt(
            proof_protocol="zk-SNARK",
            public_inputs_hash="c" * 64,
            verifier_key_id="key1",
            cryptographic_blob="blob1",
        ),
    )
    ledger = EpistemicLedgerState(history=[intent])

    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()
    actuator_engine.execute.return_value = {"status": "ok"}

    from coreason_manifest.spec.ontology import ToolManifest

    action_space = ActionSpaceManifest(
        action_space_id="test_space",
        native_tools=[
            ToolManifest.model_construct(
                tool_name="existing_tool",
                description="test",
                input_schema={"type": "object", "properties": {}},
                side_effects={"impacts_state": False, "mutates_external_systems": False},  # type: ignore[arg-type]
                permissions={"network_access": False, "file_system_access": False, "allowed_domains": []},  # type: ignore[arg-type]
            )
        ],
    )
    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
        action_space_registry={"test_space": action_space},
    )

    result = await orchestrator.tick()
    # Print debug info
    print(f"Ledger history after tick: {[e.type for e in orchestrator.ledger.history]}")
    assert result is True

    # Validate ObservationEvent wrapping InterventionIntent was appended
    assert len(orchestrator.ledger.history) == 2
    observation = orchestrator.ledger.history[-1]

    assert isinstance(observation, ObservationEvent)

    payload = observation.payload
    assert isinstance(payload, dict)
    assert payload.get("type") == "request"
    assert payload.get("target_node_id") == "did:coreason:node:a"

    context_summary = payload.get("context_summary", "")
    assert isinstance(context_summary, str)
    assert "requires approval" in context_summary

    proposed_action = payload.get("proposed_action", {})
    assert isinstance(proposed_action, dict)
    assert proposed_action.get("event_id") == intent.event_id

    # The kinetic plane should NOT have been called
    actuator_engine.execute.assert_not_called()


@pytest.mark.asyncio
async def test_tick_intervention_policy_resume_with_receipt() -> None:
    """Verifies that a pending tool is executed when an InterventionReceipt approves it."""
    import uuid

    from coreason_manifest.spec.ontology import (
        InterventionIntent,
        InterventionPolicy,
        InterventionReceipt,
        ObservationEvent,
    )

    from coreason_orchestrator.factory import EventFactory

    policy = InterventionPolicy(trigger="before_tool_execution", blocking=True)
    node_a = AgentNodeProfile(
        description="A",
        architectural_intent=".",
        justification=".",
        type="agent",
        action_space_id="test_space",
        intervention_policies=[policy],
    )
    workflow = get_mock_workflow()
    new_topo = workflow.topology.model_copy(update={"nodes": {"did:coreason:node:a": node_a}})
    workflow = workflow.model_copy(update={"topology": new_topo})

    intent_id = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
    intent = ToolInvocationEvent(
        event_id=intent_id,
        timestamp=1.0,
        type="tool_invocation",
        tool_name="existing_tool",
        parameters={"test_param": "test_value"},
        authorized_budget_magnitude=1,
        agent_attestation=AgentAttestationReceipt(
            training_lineage_hash="a" * 64,
            developer_signature="sig",
            capability_merkle_root="b" * 64,
            credential_presentations=[],
        ),
        zk_proof=ZeroKnowledgeReceipt(
            proof_protocol="zk-SNARK",
            public_inputs_hash="c" * 64,
            verifier_key_id="key1",
            cryptographic_blob="blob1",
        ),
    )

    intervention_intent = EventFactory.build_event(
        InterventionIntent,
        target_node_id="did:coreason:node:a",
        context_summary="Tool execution for 'existing_tool' requires approval.",
        proposed_action={"event_id": intent_id},
        adjudication_deadline=1.0,
    )

    # Note: intervention_request_id must match intent.event_id exactly, so we use UUID(intent.event_id[:32]) if needed,
    # but intent_id is 64 hex characters. UUID expects a 32-character hex string.
    # The intercept logic uses `str(event.intervention_request_id) == intent.event_id`.
    # Let's create an intent ID that is a valid UUID string for this test.
    uuid_str = str(uuid.uuid4())
    intent = intent.model_copy(update={"event_id": uuid_str})
    intervention_intent = intervention_intent.model_copy(update={"proposed_action": {"event_id": uuid_str}})

    intervention_receipt = EventFactory.build_event(
        InterventionReceipt,
        intervention_request_id=uuid_str,
        target_node_id="did:coreason:node:a",
        approved=True,
        feedback="Looks good",
    )

    import json

    payload_intent = json.loads(intervention_intent.model_dump_json())
    obs_intent = EventFactory.build_event(
        ObservationEvent, timestamp=1.0, type="observation", payload=payload_intent, source_node_id=None
    )

    payload_receipt = json.loads(intervention_receipt.model_dump_json())
    payload_receipt["intervention_request_id"] = intent.event_id  # ensure strict match

    payload_receipt["intervention_request_id"] = intent.event_id
    payload_receipt["approved"] = True
    obs_receipt = EventFactory.build_event(
        ObservationEvent, timestamp=2.0, type="observation", payload=payload_receipt, source_node_id=None
    )

    ledger = EpistemicLedgerState(history=[intent, obs_intent, obs_receipt])

    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()
    actuator_engine.execute.return_value = {"status": "ok"}

    from coreason_manifest.spec.ontology import ToolManifest

    action_space = ActionSpaceManifest(
        action_space_id="test_space",
        native_tools=[
            ToolManifest.model_construct(
                tool_name="existing_tool",
                description="test",
                input_schema={"type": "object", "properties": {}},
                side_effects={"impacts_state": False, "mutates_external_systems": False},  # type: ignore[arg-type]
                permissions={"network_access": False, "file_system_access": False, "allowed_domains": []},  # type: ignore[arg-type]
            )
        ],
    )
    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
        action_space_registry={"test_space": action_space},
    )

    await orchestrator.tick()
    # Print debug info
    print(f"Ledger history after tick: {[e.type for e in orchestrator.ledger.history]}")

    # Wait for tasks to finish
    await asyncio.sleep(0.1)

    # Verify that the kinetic engine was called!
    actuator_engine.execute.assert_called_once()


@pytest.mark.asyncio
async def test_tick_intervention_policy_wait_for_receipt() -> None:
    """Verifies that a pending tool waits for an InterventionReceipt if InterventionIntent is already emitted."""
    import uuid

    from coreason_manifest.spec.ontology import InterventionIntent, InterventionPolicy, ObservationEvent

    from coreason_orchestrator.factory import EventFactory

    policy = InterventionPolicy(trigger="before_tool_execution", blocking=True)
    node_a = AgentNodeProfile(
        description="A",
        architectural_intent=".",
        justification=".",
        type="agent",
        action_space_id="test_space",
        intervention_policies=[policy],
    )
    workflow = get_mock_workflow()
    new_topo = workflow.topology.model_copy(update={"nodes": {"did:coreason:node:a": node_a}})
    workflow = workflow.model_copy(update={"topology": new_topo})

    intent_id = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
    intent = ToolInvocationEvent(
        event_id=intent_id,
        timestamp=1.0,
        type="tool_invocation",
        tool_name="existing_tool",
        parameters={"test_param": "test_value"},
        authorized_budget_magnitude=1,
        agent_attestation=AgentAttestationReceipt(
            training_lineage_hash="a" * 64,
            developer_signature="sig",
            capability_merkle_root="b" * 64,
            credential_presentations=[],
        ),
        zk_proof=ZeroKnowledgeReceipt(
            proof_protocol="zk-SNARK",
            public_inputs_hash="c" * 64,
            verifier_key_id="key1",
            cryptographic_blob="blob1",
        ),
    )

    intervention_intent = EventFactory.build_event(
        InterventionIntent,
        target_node_id="did:coreason:node:a",
        context_summary="Tool execution for 'existing_tool' requires approval.",
        proposed_action={"event_id": intent_id},
        adjudication_deadline=1.0,
    )

    uuid_str = str(uuid.uuid4())
    intent = intent.model_copy(update={"event_id": uuid_str})
    intervention_intent = intervention_intent.model_copy(update={"proposed_action": {"event_id": uuid_str}})

    import json

    payload_intent = json.loads(intervention_intent.model_dump_json())
    obs_intent = EventFactory.build_event(
        ObservationEvent, timestamp=1.0, type="observation", payload=payload_intent, source_node_id=None
    )

    # Note: No obs_receipt is added to ledger!
    ledger = EpistemicLedgerState(history=[intent, obs_intent])

    inference_engine = AsyncMock()
    actuator_engine = AsyncMock()
    actuator_engine.execute.return_value = {"status": "ok"}

    from coreason_manifest.spec.ontology import ToolManifest

    action_space = ActionSpaceManifest(
        action_space_id="test_space",
        native_tools=[
            ToolManifest.model_construct(
                tool_name="existing_tool",
                description="test",
                input_schema={"type": "object", "properties": {}},
                side_effects={"impacts_state": False, "mutates_external_systems": False},  # type: ignore[arg-type]
                permissions={"network_access": False, "file_system_access": False, "allowed_domains": []},  # type: ignore[arg-type]
            )
        ],
    )
    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,
        actuator_engine=actuator_engine,
        action_space_registry={"test_space": action_space},
    )

    await orchestrator.tick()

    import asyncio

    await asyncio.sleep(0.1)

    # Should NOT have executed
    actuator_engine.execute.assert_not_called()
