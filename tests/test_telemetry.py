import json
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
    BeliefMutationEvent,
    EpistemicLedgerState,
    EpistemicProvenanceReceipt,
    GraphFlatteningPolicy,
    SemanticEdgeState,
    SemanticNodeState,
)
from hypothesis import given, settings
from hypothesis.strategies import booleans, sampled_from

from coreason_orchestrator.telemetry import async_serialize_ledger


@pytest.mark.asyncio
async def test_async_serialize_ledger_runs_in_thread() -> None:
    """Verifies that the massive ledger serialization runs inside asyncio.to_thread to prevent event loop starvation."""
    ledger = EpistemicLedgerState(history=[])
    policy = GraphFlatteningPolicy(
        node_projection_mode="wide_columnar",
        edge_projection_mode="adjacency_matrix",
        preserve_cryptographic_lineage=True,
    )

    with patch("coreason_orchestrator.telemetry.asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread:
        mock_to_thread.return_value = '{"test": "flattened"}'
        result = await async_serialize_ledger(ledger, policy)

        # Assert asyncio.to_thread was called with the sync serialization function
        mock_to_thread.assert_called_once()
        args, _ = mock_to_thread.call_args
        assert args[0].__name__ == "_serialize_ledger_sync"
        assert args[1] == ledger
        assert args[2] == policy

        assert result == '{"test": "flattened"}'


@pytest.mark.asyncio
async def test_async_serialize_ledger_output() -> None:
    """Verifies the actual output of the async serialization wrapper matches the ledger dump."""
    node = SemanticNodeState(
        node_id="did:coreason:node:1",
        label="TestNode",
        text_chunk="Hello",
        provenance=EpistemicProvenanceReceipt(extracted_by="did:coreason:node:agent1", source_event_id="e0"),
    )
    edge = SemanticEdgeState(
        edge_id="did:coreason:edge:1",
        subject_node_id="did:coreason:node:1",
        object_node_id="did:coreason:node:2",
        confidence_score=0.9,
        predicate="knows",
        provenance=EpistemicProvenanceReceipt(extracted_by="did:coreason:node:agent1", source_event_id="e0"),
    )
    event = BeliefMutationEvent(
        event_id="e1",
        timestamp=1.0,
        type="belief_mutation",
        payload={
            "n1": node.model_dump(),
            "e1": edge.model_dump(),
        },
    )
    ledger = EpistemicLedgerState(history=[event])
    policy = GraphFlatteningPolicy(
        node_projection_mode="wide_columnar",
        edge_projection_mode="adjacency_matrix",
        preserve_cryptographic_lineage=True,
    )

    # We await the async_serialize_ledger to run real logic in thread
    result = await async_serialize_ledger(ledger, policy)

    parsed = json.loads(result)
    assert "history" in parsed
    assert len(parsed["history"]) == 1
    assert parsed["history"][0]["event_id"] == "e1"

    assert "projected_nodes" in parsed
    assert "did:coreason:node:1" in parsed["projected_nodes"]
    assert parsed["projected_nodes"]["did:coreason:node:1"]["node_id"] == "did:coreason:node:1"

    assert "projected_edges" in parsed
    assert "did:coreason:node:1" in parsed["projected_edges"]
    assert len(parsed["projected_edges"]["did:coreason:node:1"]) == 1
    assert parsed["projected_edges"]["did:coreason:node:1"][0]["edge_id"] == "did:coreason:edge:1"


@settings(max_examples=10)  # type: ignore
@given(  # type: ignore
    node_mode=sampled_from(["wide_columnar", "struct_array"]),
    edge_mode=sampled_from(["adjacency_matrix", "map_array"]),
    preserve_lineage=booleans(),
)
@pytest.mark.asyncio
async def test_async_serialize_ledger_policy_application(
    node_mode: str, edge_mode: str, preserve_lineage: bool
) -> None:
    """Property-based testing to ensure varying policies are successfully processed."""
    node = SemanticNodeState(
        node_id="did:coreason:node:1",
        label="TestNode",
        text_chunk="Hello",
        provenance=EpistemicProvenanceReceipt(extracted_by="did:coreason:node:agent1", source_event_id="e0"),
    )
    edge = SemanticEdgeState(
        edge_id="did:coreason:edge:1",
        subject_node_id="did:coreason:node:1",
        object_node_id="did:coreason:node:2",
        confidence_score=0.9,
        predicate="knows",
        provenance=EpistemicProvenanceReceipt(extracted_by="did:coreason:node:agent1", source_event_id="e0"),
    )
    event = BeliefMutationEvent(
        event_id="e1",
        timestamp=1.0,
        type="belief_mutation",
        payload={
            "n1": node.model_dump(),
            "e1": edge.model_dump(),
            "invalid_node": {"node_id": "bad", "label": "missing_chunk"},
            "invalid_edge": {"edge_id": "bad", "subject_node_id": "subj"},
            "primitive": "just_a_string",
            "exception_node": {"node_id": "bad2", "label": "bad2", "text_chunk": "bad2", "provenance": "invalid"},
            "exception_edge": {
                "edge_id": "bad3",
                "subject_node_id": "subj",
                "object_node_id": "obj",
                "provenance": "invalid",
            },
        },
    )
    ledger = EpistemicLedgerState(history=[event])
    policy = GraphFlatteningPolicy(
        node_projection_mode=node_mode,  # type: ignore
        edge_projection_mode=edge_mode,  # type: ignore
        preserve_cryptographic_lineage=preserve_lineage,
    )

    result = await async_serialize_ledger(ledger, policy)
    assert result != ""
    parsed = json.loads(result)
    assert "history" in parsed

    if not preserve_lineage:
        assert "event_id" not in parsed["history"][0]

    # Check node projection
    assert "projected_nodes" in parsed
    if node_mode == "struct_array":
        assert isinstance(parsed["projected_nodes"], list)
        assert len(parsed["projected_nodes"]) == 1
        if preserve_lineage:
            assert parsed["projected_nodes"][0]["node_id"] == "did:coreason:node:1"
        else:
            assert "node_id" not in parsed["projected_nodes"][0]
    else:
        assert isinstance(parsed["projected_nodes"], dict)
        # Even if preserve_lineage=False, the dict keys for wide_columnar might still be node_id
        # (as per our implementation dict is {node.node_id: node_dump})
        # but the node_id inside the payload should be stripped
        assert "did:coreason:node:1" in parsed["projected_nodes"]
        if not preserve_lineage:
            assert "node_id" not in parsed["projected_nodes"]["did:coreason:node:1"]

    # Check edge projection
    assert "projected_edges" in parsed
    if edge_mode == "map_array":
        assert isinstance(parsed["projected_edges"], list)
        assert len(parsed["projected_edges"]) == 1
        if preserve_lineage:
            assert parsed["projected_edges"][0]["edge_id"] == "did:coreason:edge:1"
        else:
            assert "edge_id" not in parsed["projected_edges"][0]
    else:
        assert isinstance(parsed["projected_edges"], dict)
        assert "did:coreason:node:1" in parsed["projected_edges"]
        if not preserve_lineage:
            assert "edge_id" not in parsed["projected_edges"]["did:coreason:node:1"][0]
