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
    DAGTopologyManifest,
    EpistemicLedgerState,
    ObservationEvent,
    WorkflowManifest,
)


def resolve_current_node(workflow: WorkflowManifest, ledger: EpistemicLedgerState) -> AgentNodeProfile | None:
    """
    Evaluates the AnyTopologyManifest discriminator within the root WorkflowManifest
    to deterministically resolve the current active AgentNodeProfile based on the
    most recent events in the ledger.

    Args:
        workflow: The root WorkflowManifest containing the strict topological contract.
        ledger: The cryptographically sealed EpistemicLedgerState.

    Returns:
        The strictly typed AgentNodeProfile representing the active cursor, or None if terminal.
    """
    topology = workflow.topology

    # Only DAGTopologyManifest has explicit edges to traverse in this simple implementation
    if not isinstance(topology, DAGTopologyManifest):
        # For non-DAG topologies, or empty node sets, we might not resolve a deterministic single node
        # We can default to the first available node if present
        if hasattr(topology, "nodes") and topology.nodes:
            first_key = next(iter(topology.nodes))
            node = topology.nodes[first_key]
            if isinstance(node, AgentNodeProfile):
                return node
        return None

    nodes = topology.nodes
    edges = topology.edges

    if not nodes:
        return None

    # Determine nodes with in-degree 0 (roots)
    has_incoming = {dst for _, dst in edges}
    roots = [node_id for node_id in nodes if node_id not in has_incoming]

    # Sort for deterministic resolution
    roots.sort()

    if not roots:
        # Cyclic without a clear root or empty graph
        first_key = next(iter(nodes))
        node = nodes[first_key]
        if isinstance(node, AgentNodeProfile):
            return node
        return None

    # Track execution history
    # A node is considered completed if there is an EpistemicFlowStateReceipt
    # or ObservationEvent indicating terminal state
    # Actually, the FRD mentions EpistemicFlowStateReceipt is terminal completion.
    completed_nodes: set[str] = set()
    for event in ledger.history:
        if isinstance(event, ObservationEvent) and event.source_node_id:
            # We might consider ObservationEvent as a state event that implies execution happened on the node
            completed_nodes.add(event.source_node_id)
        # Note: EpistemicFlowStateReceipt does not hold a source_node_id directly in the current ontology version,
        # but ObservationEvent strictly binds source_node_id.

    # BFS or simple linear topological search
    # Find the first node that hasn't been completed but whose predecessors are all completed

    # Build adjacency list
    adj: dict[str, list[str]] = {str(n): [] for n in nodes}
    for src, dst in edges:
        adj[str(src)].append(str(dst))

    queue = [str(r) for r in roots]

    # This is a basic topological traversal to find the frontier
    while queue:
        # Pop deterministically
        queue.sort()
        curr = queue.pop(0)

        if curr not in completed_nodes:
            node = nodes[curr]
            if isinstance(node, AgentNodeProfile):
                return node
            return None

        # If completed, add neighbors
        queue.extend(adj.get(curr, []))

    # All nodes are completed
    return None
