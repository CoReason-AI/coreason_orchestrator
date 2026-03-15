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
    AgentNodeProfile,
    DAGTopologyManifest,
    EpistemicLedgerState,
    EpistemicProvenanceReceipt,
    ObservationEvent,
    TokenBurnReceipt,
    WorkflowManifest,
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
