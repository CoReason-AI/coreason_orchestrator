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
    EpistemicLedgerState,
    GraphFlatteningPolicy,
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
    ledger = EpistemicLedgerState(history=[])
    policy = GraphFlatteningPolicy(
        node_projection_mode="wide_columnar",
        edge_projection_mode="adjacency_matrix",
        preserve_cryptographic_lineage=True,
    )

    # We await the async_serialize_ledger to run real logic in thread
    result = await async_serialize_ledger(ledger, policy)

    parsed = json.loads(result)
    assert "history" in parsed
    assert parsed["history"] == []


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
    ledger = EpistemicLedgerState(history=[])
    policy = GraphFlatteningPolicy(
        node_projection_mode=node_mode,  # type: ignore
        edge_projection_mode=edge_mode,  # type: ignore
        preserve_cryptographic_lineage=preserve_lineage,
    )

    result = await async_serialize_ledger(ledger, policy)
    assert result != ""
    parsed = json.loads(result)
    assert "history" in parsed
