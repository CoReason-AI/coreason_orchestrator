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
    EpistemicFlowStateReceipt,
    EpistemicLedgerState,
    ObservationEvent,
    WorkflowManifest,
)


def resolve_current_node(workflow: WorkflowManifest, ledger: EpistemicLedgerState) -> list[AgentNodeProfile]:
    """
    Evaluates the AnyTopologyManifest discriminator within the root WorkflowManifest
    to deterministically resolve the active execution frontier based on the
    most recent events in the ledger.

    Args:
        workflow: The root WorkflowManifest containing the strict topological contract.
        ledger: The cryptographically sealed EpistemicLedgerState.

    Returns:
        A list of strictly typed AgentNodeProfile objects representing the active cursor.
        Returns an empty list if terminal.
    """
    topology = workflow.topology

    # Only DAGTopologyManifest has explicit edges to traverse in this simple implementation
    if not isinstance(topology, DAGTopologyManifest):
        # For non-DAG topologies, or empty node sets, we might not resolve a deterministic frontier
        # We can default to all available agent nodes if present
        frontier: list[AgentNodeProfile] = []
        if hasattr(topology, "nodes") and topology.nodes:
            for node_id in sorted(topology.nodes.keys()):
                node = topology.nodes[node_id]
                if isinstance(node, AgentNodeProfile):
                    frontier.append(node)
        return frontier

    nodes = topology.nodes
    edges = topology.edges

    if not nodes:
        return []

    # Determine nodes with in-degree 0 (roots)
    has_incoming = {dst for _, dst in edges}
    roots = [node_id for node_id in nodes if node_id not in has_incoming]

    # Sort for deterministic resolution
    roots.sort()

    if not roots:
        # Cyclic without a clear root or empty graph
        first_key = next(iter(sorted(nodes.keys())))
        node = nodes[first_key]
        if isinstance(node, AgentNodeProfile):
            return [node]
        return []

    # Track execution history and routing decisions
    # A node is considered completed if there is an EpistemicFlowStateReceipt
    # or ObservationEvent indicating terminal state
    # Actually, the FRD mentions EpistemicFlowStateReceipt is terminal completion.
    completed_nodes: set[str] = set()
    active_subgraphs_set: set[str] = set()
    has_routing_manifest = False

    for event in ledger.history:
        if isinstance(event, EpistemicFlowStateReceipt):
            if event.source_trajectory_id:
                completed_nodes.add(event.source_trajectory_id)
        elif isinstance(event, ObservationEvent):
            if event.source_node_id:
                # We might consider ObservationEvent as a state event that implies execution happened on the node
                completed_nodes.add(event.source_node_id)

            # FR-2.2 Conditional Edges: Extract embedded DynamicRoutingManifest from the ObservationEvent natively.
            embedded_routing_manifest = getattr(event, "embedded_routing_manifest", None)
            if embedded_routing_manifest is not None:
                has_routing_manifest = True

                for step in embedded_routing_manifest.bypassed_steps:
                    completed_nodes.add(step.bypassed_node_id)

                for subgraph_nodes in embedded_routing_manifest.active_subgraphs.values():
                    for n in subgraph_nodes:
                        active_subgraphs_set.add(n)

    # Build adjacency list (forward edges) and in-degree tracking (predecessors)
    adj: dict[str, list[str]] = {str(n): [] for n in nodes}
    preds: dict[str, set[str]] = {str(n): set() for n in nodes}
    for src, dst in edges:
        adj[str(src)].append(str(dst))
        preds[str(dst)].add(str(src))

    active_frontier: list[AgentNodeProfile] = []

    # To find all nodes in the frontier:
    # A node is in the frontier if it is NOT completed, but ALL its predecessors ARE completed.
    for node_id in sorted(nodes.keys()):
        if node_id in completed_nodes:
            continue

        # FR-2.2 Conditional Edges: Delegate conditional routing to DynamicRoutingManifest.
        # If any routing manifests exist and define active_subgraphs,
        # only nodes explicitly listed in active_subgraphs should be explored.
        if has_routing_manifest and active_subgraphs_set and node_id not in active_subgraphs_set:
            continue

        # Check if all predecessors are completed
        predecessors_completed = True
        for pred in preds[node_id]:
            if pred not in completed_nodes:
                predecessors_completed = False
                break

        if predecessors_completed:
            node = nodes[node_id]
            if isinstance(node, AgentNodeProfile):
                active_frontier.append(node)

    return active_frontier
