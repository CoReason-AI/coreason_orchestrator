# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator

import asyncio
import contextlib
import json
import os
from typing import Any

import httpx
from coreason_manifest.spec.ontology import (
    BeliefMutationEvent,
    EpistemicLedgerState,
    ExecutionSpanReceipt,
    GraphFlatteningPolicy,
    SemanticEdgeState,
    SemanticNodeState,
    TraceExportManifest,
)


class OTelBatchExporter:
    """
    Exports a batch of ExecutionSpanReceipts to an external OTLP endpoint asynchronously.
    """

    def __init__(self) -> None:
        self.endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318/v1/traces")
        self.headers = {"Content-Type": "application/json"}
        env_headers = os.environ.get("OTEL_EXPORTER_OTLP_HEADERS")
        if env_headers:  # pragma: no cover
            for pair in env_headers.split(","):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    self.headers[k.strip()] = v.strip()

    async def flush_spans(self, spans: list[ExecutionSpanReceipt]) -> None:  # pragma: no cover
        if not spans:
            return

        import secrets

        batch_id = secrets.token_hex(64)  # 128-char CID
        manifest = TraceExportManifest(batch_id=batch_id, spans=spans)

        async with httpx.AsyncClient(timeout=5.0) as client:
            with contextlib.suppress(Exception):
                await client.post(self.endpoint, headers=self.headers, content=manifest.model_dump_json())


def _extract_nodes_edges(ledger: EpistemicLedgerState) -> tuple[list[SemanticNodeState], list[SemanticEdgeState]]:
    nodes: list[SemanticNodeState] = []
    edges: list[SemanticEdgeState] = []

    for event in ledger.history:
        if isinstance(event, BeliefMutationEvent):
            for value in event.payload.values():
                if isinstance(value, dict):
                    # Rough heuristic for semantic objects
                    if value.get("node_id") and value.get("label") and value.get("text_chunk"):
                        with contextlib.suppress(Exception):
                            nodes.append(SemanticNodeState.model_validate(value))
                    elif value.get("edge_id") and value.get("subject_node_id") and value.get("object_node_id"):
                        with contextlib.suppress(Exception):
                            edges.append(SemanticEdgeState.model_validate(value))
    return nodes, edges


def _strip_lineage(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {
            k: _strip_lineage(v)
            for k, v in obj.items()
            if k not in ("event_id", "diff_id", "node_id", "edge_id", "checkpoint_id")
        }
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(_strip_lineage(item) for item in obj)
    return obj


def _serialize_ledger_sync(ledger: EpistemicLedgerState, policy: GraphFlatteningPolicy) -> str:
    """
    Synchronously serializes the massive EpistemicLedgerState based on the GraphFlatteningPolicy.
    This function is computationally expensive and MUST run inside a separate thread.
    """
    # Create the full base dump preserving all fields like active_rollbacks, etc.
    base_dump = ledger.model_dump(mode="json")

    # 1. Strip cryptographic lineage if mandated
    if not policy.preserve_cryptographic_lineage:
        base_dump = _strip_lineage(base_dump)

    # 2. Extract and project Semantic Nodes and Edges
    nodes, edges = _extract_nodes_edges(ledger)

    if policy.node_projection_mode == "struct_array":
        nodes_dump = [node.model_dump() for node in nodes]
        base_dump["projected_nodes"] = (
            _strip_lineage(nodes_dump) if not policy.preserve_cryptographic_lineage else nodes_dump
        )
    elif policy.node_projection_mode == "wide_columnar":
        nodes_dump_dict = {node.node_id: node.model_dump() for node in nodes}
        base_dump["projected_nodes"] = (
            _strip_lineage(nodes_dump_dict) if not policy.preserve_cryptographic_lineage else nodes_dump_dict
        )

    if policy.edge_projection_mode == "map_array":
        edges_dump = [edge.model_dump() for edge in edges]
        base_dump["projected_edges"] = (
            _strip_lineage(edges_dump) if not policy.preserve_cryptographic_lineage else edges_dump
        )
    elif policy.edge_projection_mode == "adjacency_matrix":
        adj: dict[str, list[dict[str, Any]]] = {}
        for edge in edges:
            subj = edge.subject_node_id
            if subj not in adj:
                adj[subj] = []
            adj[subj].append(edge.model_dump())
        base_dump["projected_edges"] = _strip_lineage(adj) if not policy.preserve_cryptographic_lineage else adj

    # 3. Serialize massive ledger deterministically via json.dumps
    return json.dumps(base_dump)


async def async_serialize_ledger(ledger: EpistemicLedgerState, policy: GraphFlatteningPolicy) -> str:
    """
    Asynchronously delegates the massive N-dimensional topological flattening and
    ledger serialization to a background thread to prevent event loop starvation.

    Strictly utilizes PEP 703 NoGIL threading over ProcessPoolExecutor to avoid IPC
    pickling overhead.

    Args:
        ledger: The current massive EpistemicLedgerState to serialize.
        policy: The GraphFlatteningPolicy dictating serialization rules.

    Returns:
        The JSON string representation of the flattened ledger.
    """
    res = await asyncio.to_thread(_serialize_ledger_sync, ledger, policy)
    return str(res)
