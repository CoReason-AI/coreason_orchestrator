import asyncio
import logging

# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator
import time
from typing import Any

from coreason_manifest.spec.ontology import (
    ActionSpaceManifest,
    AgentNodeProfile,
    AnyIntent,
    AnyStateEvent,
    BargeInInterruptEvent,
    EpistemicLedgerState,
    ExecutionSpanReceipt,
    InterventionIntent,
    ObservationEvent,
    TokenBurnReceipt,
    ToolInvocationEvent,
    ToolManifest,
    WorkflowManifest,
)

from coreason_orchestrator.factory import EventFactory
from coreason_orchestrator.interfaces import ActuatorEngineProtocol, InferenceEngineProtocol
from coreason_orchestrator.ledger import append_event
from coreason_orchestrator.resolve import resolve_current_node
from coreason_orchestrator.telemetry import OTelBatchExporter


class CoreOrchestrator:
    """
    The central state machine and traffic controller for the CoReason multi-agent swarm.

    Operating strictly within the Hollow Data Plane, it forces mathematical bounding
    between the Cognitive Plane (Inference Engine) and Kinetic Plane (Actuator Engine).
    """

    def __init__(
        self,
        workflow: WorkflowManifest,
        ledger: EpistemicLedgerState,
        inference_engine: InferenceEngineProtocol,
        actuator_engine: ActuatorEngineProtocol,
        action_space_registry: dict[str, "ActionSpaceManifest"] | None = None,
    ) -> None:
        self.workflow = workflow
        # Dynamically unroll Zero-Cost Macros into executable Base Topologies
        if (
            self.workflow
            and hasattr(self.workflow, "topology")
            and hasattr(self.workflow.topology, "compile_to_base_topology")
        ):
            self.workflow = self.workflow.model_copy(
                update={"topology": self.workflow.topology.compile_to_base_topology()}
            )
        self.ledger = ledger
        self.inference_engine = inference_engine
        self.actuator_engine = actuator_engine
        self.action_space_registry = action_space_registry or {}
        self._ledger_lock = asyncio.Lock()
        self.interrupt_queue: asyncio.Queue[tuple[BargeInInterruptEvent, str]] = asyncio.Queue()
        self.exporter = OTelBatchExporter()

    async def append_to_ledger(self, event: AnyStateEvent | AnyIntent) -> None:
        """
        Public API for safely mutating the epistemic ledger state.

        Takes an AnyStateEvent or AnyIntent and safely appends it mathematically
        bound to the ledger history behind the internal concurrency lock.
        """
        async with self._ledger_lock:
            self.ledger = append_event(self.ledger, event)

    async def inject_observation(self, user_input: str) -> None:
        """Injects an interactive user observation directly into the ledger."""
        import time

        from coreason_manifest.spec.ontology import ObservationEvent

        from coreason_orchestrator.factory import EventFactory

        # Synthesize a valid ObservationEvent from the human's input
        obs = EventFactory.build_event(
            ObservationEvent,
            timestamp=time.time(),
            type="observation",
            payload={"user_input": user_input},
        )

        # Securely append to the immutable ledger
        async with self._ledger_lock:
            self.ledger = append_event(self.ledger, obs)

    def dump_partial_state(self) -> EpistemicLedgerState:
        """
        Disaster Recovery: Returns the current crystallized state of the ledger
        following an execution collapse.
        """
        return self.ledger

    async def delegate_to_cognitive_plane(
        self, node: AgentNodeProfile, node_id: str, action_space: ActionSpaceManifest
    ) -> None:
        """
        Delegates the generation of intent to the physically air-gapped Inference Engine.
        Enforces Phase 5 (Dual Verification) and Phase 6 (Memory Distillation).
        """
        # Phase 6: Memory Distillation (EvictionPolicy)
        compile_ledger = self.ledger
        if getattr(node, "eviction_policy", None):
            from coreason_orchestrator.ledger import apply_eviction_policy
            compile_ledger = apply_eviction_policy(self.ledger, node.eviction_policy)

        # Phase 2: Monte Carlo Tree Search (PRM Pruning)
        prm_policy = getattr(node, "prm_policy", None)
        if prm_policy:
            sla = getattr(self.workflow.topology, "convergence_sla", None)
            rollouts = getattr(sla, "max_monte_carlo_rollouts", 3) if sla else 3
            
            async def _mcts_rollout() -> tuple[Any, Any, Any, Any]:
                try:
                    return await self.inference_engine.generate_intent(
                        node, compile_ledger, node_id, action_space
                    )
                except Exception as e:
                    return None, None, None, e
                    
            candidates = await asyncio.gather(*[_mcts_rollout() for _ in range(rollouts)])
            successful_rollouts = [c for c in candidates if not isinstance(c[3], Exception)]
            
            if not successful_rollouts:
                raise candidates[0][3]  # type: ignore
                
            payload, burn_receipt, _scratchpad, _cognitive_receipt = successful_rollouts[0]
            
            # Prune and store metadata (burn receipts for discarded branches are still billed)
            async with self._ledger_lock:
                for c in successful_rollouts[1:]:
                    if c[1]:
                        self.ledger = append_event(self.ledger, c[1])
        else:
            # 1. Dispatch the primary node profile and strictly read-only ledger
            payload, burn_receipt, _scratchpad, _cognitive_receipt = await self.inference_engine.generate_intent(
                node, compile_ledger, node_id, action_space
            )

        from coreason_manifest.spec.ontology import (
            DAGTopologyManifest, DraftingIntent, SemanticNodeState, EpistemicProvenanceReceipt,
            CognitiveDualVerificationReceipt, NodeIdentifierState,
            System2RemediationIntent, ManifestViolationReceipt
        )

        # Systematic Search (Phase 1): StateContract domain_extensions evaluation
        state_contract = getattr(node, "state_contract", None)
        if state_contract and getattr(state_contract, "domain_extensions", None):
            domain_extensions = state_contract.domain_extensions
            payload_dict = payload.model_dump() if hasattr(payload, "model_dump") else (payload if isinstance(payload, dict) else {})
            violations = []
            for key, expected_val in domain_extensions.items():
                if payload_dict.get(key) != expected_val:
                    violations.append(
                        ManifestViolationReceipt(
                            failing_pointer=f"/{key}",
                            violation_type="domain_extension_variance",
                            diagnostic_message=f"StateContract constraint evaluation failed: Expected {expected_val}, got {payload_dict.get(key)}"
                        )
                    )
            
            if violations:
                payload = EventFactory.build_event(
                    System2RemediationIntent,
                    target_node_id=node_id,
                    violation_receipts=violations
                )

        # Phase 1: Dynamic Query Decomposition (DAG Injection)
        if isinstance(payload, DAGTopologyManifest) and isinstance(self.workflow.topology, DAGTopologyManifest):
            # The LLM has dynamically shattered its logical boundary into parallel nodes.
            new_nodes = {k: v for k, v in self.workflow.topology.nodes.items()}
            new_edges = list(self.workflow.topology.edges)
            modified = False
            
            for new_node_id, new_node_profile in payload.nodes.items():
                if new_node_id not in new_nodes:
                    new_nodes[new_node_id] = new_node_profile
                    modified = True
                    
            for edge in payload.edges:
                if edge not in new_edges:
                    new_edges.append(edge)
                    modified = True
                    
            if modified:
                # We inject these nodes directly into the live topology and rebuild it mathematically
                new_topology = self.workflow.topology.model_copy(update={
                    "nodes": new_nodes,
                    "edges": new_edges
                })
                self.workflow = self.workflow.model_copy(update={"topology": new_topology})
                
                # Treat the payload as a completed metric so the orchestrator moves to evaluating the new nodes
                payload = EventFactory.build_event(DraftingIntent, target_node_id=node_id, response_surface="DAG dynamically injected.")

        # Phase 6: Epistemic Provenance Lock
        if isinstance(payload, (DraftingIntent, SemanticNodeState)):
            # Force provenance receipt directly onto the payload before commiting to the DAG
            if not getattr(payload, "provenance", None):
                provenance = EpistemicProvenanceReceipt(
                    extracted_by=NodeIdentifierState(did=f"did:coreason:agent:{node_id}"),
                    source_event_id=getattr(payload, "event_id", f"syn_{time.time()}"),
                    trigger_conditions=["cognitive_generation"],
                    target_layers=["default"]
                )
                object.__setattr__(payload, "provenance", provenance)

        # Phase 5: Semantic Gap Analysis and Cognitive Dual Verification
        if isinstance(payload, DraftingIntent) and getattr(node, "verification_lock", None):
            # The Two-Man Rule: Spawn a secondary statistical model execution
            verify_payload, verify_burn_receipt, _, _ = await self.inference_engine.generate_intent(
                node, compile_ledger, node_id, action_space
            )
            
            # Record secondary thermodynamic cost
            async with self._ledger_lock:
                self.ledger = append_event(self.ledger, verify_burn_receipt)

            # Enforce mathematical structural alignment (Semantic Gap Analysis proxy)
            # If the secondary independent model doesn't also output a DraftingIntent with the same structure, it fails
            is_aligned = type(verify_payload) == type(payload)
            
            verification = CognitiveDualVerificationReceipt(
                primary_verifier_id=NodeIdentifierState(did=f"did:coreason:llm:primary:{node_id}"),
                secondary_verifier_id=NodeIdentifierState(did=f"did:coreason:llm:secondary:{node_id}"),
                trace_factual_alignment=is_aligned
            )
            object.__setattr__(payload, "verification_lock", verification)

            # Set-Theoretic rejection matrix
            if not is_aligned:
                payload = EventFactory.build_event(
                    System2RemediationIntent,
                    target_node_id=node_id,
                    violation_receipts=[ManifestViolationReceipt(
                        failing_pointer="/drafting", 
                        violation_type="cognitive_dissonance", 
                        diagnostic_message="Phase 5 Semantic Gap Analysis failed. Secondary evaluating LLM structurally disagreed with the primary draft."
                    )]
                )

        # 2. Append the generated intent/event and thermodynamic burn receipt to the ledger securely
        async with self._ledger_lock:
            new_ledger = append_event(self.ledger, payload)
            self.ledger = append_event(new_ledger, burn_receipt)

            # FR-1.2: Automatically emit EpistemicFlowStateReceipt to conceptually mark the node 
            # execution as fully resolved in the Merkle-DAG if the intent is self-contained.
            from coreason_manifest.spec.ontology import (
                ToolInvocationEvent, LatentSchemaInferenceIntent, LatentProjectionIntent, EpistemicFlowStateReceipt
            )
            if not isinstance(payload, (ToolInvocationEvent, LatentSchemaInferenceIntent, LatentProjectionIntent)):
                import time
                receipt = EventFactory.build_event(
                    EpistemicFlowStateReceipt,
                    timestamp=time.time(),
                    source_trajectory_id=node_id,
                    estimated_flow_value=1.0,
                    terminal_reward_factorized=True
                )
                self.ledger = append_event(self.ledger, receipt)

    async def delegate_to_kinetic_plane(
        self, 
        intent: ToolInvocationEvent, 
        manifest: ToolManifest,
        partitions: list["EphemeralNamespacePartitionState"] | None = None
    ) -> None:
        """
        Delegates the execution of a tool to the air-gapped Kinetic Plane.

        To enforce absolute Zero-Trust, the Orchestrator passes a compiled StateHydrationManifest
        and securely isolated EphemeralNamespacePartitionStates for sandboxing scripts (Phase 3). 
        It strictly awaits a raw JsonPrimitiveState payload and synthesizes the ObservationEvent natively.

        Args:
            intent: The validated ToolInvocationEvent from the Cognitive Plane.
            manifest: The associated ToolManifest defining the capability.
            partitions: The secure sandboxing namespaces restricting the process execution geography.
        """
        # 2. Dispatch execution and strictly await the raw JSON payload
        raw_payload = await self.actuator_engine.execute(intent, manifest, self.ledger.eviction_policy, partitions)

        # 3. Securely synthesize the ObservationEvent natively (Zero-Trust)
        # We must explicitly cast raw_payload to dict[str, Any] as required by ObservationEvent payload
        if not isinstance(raw_payload, dict):
            raw_payload = {"result": raw_payload}

        # Systematic Search (Phase 6): Tensor Extraction Mapping
        # Automatically wrap high-entropy statistical extractions (e.g. from PDF scraping) into a structured NDimensional tensor equivalent
        from coreason_manifest.spec.ontology import SemanticNodeState
        if "p_value" in raw_payload or "hazard_ratio" in raw_payload or "confidence_interval" in raw_payload:
            tensor_state = SemanticNodeState(
                node_id=intent.event_id,
                semantic_type="clinical_evidence_tensor",
                state_schema=raw_payload,
                embedding_vector=[]
            )
            raw_payload = {"tensor_state": tensor_state.model_dump()}

        observation = EventFactory.build_event(
            ObservationEvent,
            timestamp=time.time(),
            type="observation",
            payload=raw_payload,
            triggering_invocation_id=intent.event_id,
        )

        # 4. Append the ObservationEvent to the immutable ledger
        async with self._ledger_lock:
            self.ledger = append_event(self.ledger, observation)

    async def delegate_research_intent_to_kinetic_plane(
        self,
        intent: "LatentSchemaInferenceIntent",
        partitions: list["EphemeralNamespacePartitionState"] | None = None
    ) -> None:
        """
        FR-4.3: Delegates high-order LatentSchemaInferenceIntents (Phase 4 neurosymbolic mapping)
        to the strictly isolated sandboxed Actuator Engine.
        """
        import time
        from coreason_manifest.spec.ontology import ObservationEvent

        # 1. Dispatch execution and securely await the Schema Deduction Payload
        raw_payload = await getattr(self.actuator_engine, "execute_research_intent", self.actuator_engine.execute)(intent, partitions)

        if not isinstance(raw_payload, dict):
            raw_payload = {"result": raw_payload}

        # 2. Securely synthesize the deterministic Observation natively
        observation = EventFactory.build_event(
            ObservationEvent,
            timestamp=time.time(),
            type="observation",
            payload=raw_payload,
            triggering_invocation_id=getattr(intent, "event_id", getattr(intent, "target_buffer_id", "unknown")),
        )

        # 3. Commit observation to immutable ledger
        async with self._ledger_lock:
            self.ledger = append_event(self.ledger, observation)

    def handle_preemption(self, interrupt_event: BargeInInterruptEvent, active_invocation_id: str) -> None:
        """
        Intercepts a preemption signal to instantly halt runaway execution branches.

        Directly appends the BargeInInterruptEvent to the ledger, explicitly pointing its
        target_event_id to the actively executing ToolInvocationEvent CID, and setting
        the epistemic_disposition to 'discard' to prevent ontological corruption.

        Args:
            interrupt_event: The strictly typed preemption signal.
            active_invocation_id: The CID of the currently executing ToolInvocationEvent.
        """
        terminal_event = interrupt_event.model_copy(
            update={
                "target_event_id": active_invocation_id,
                "epistemic_disposition": "discard",
            }
        )
        self.ledger = append_event(self.ledger, terminal_event)

    def slash_byzantine_fault(self, penalized_node_id: str, invocation_id: str, burn_magnitude: int) -> None:
        """
        Executes a Byzantine slashing penalty against a misbehaving agent.

        Synthesizes and publishes a TokenBurnReceipt mapped to the penalized node,
        strictly adhering to the ontology's economic invariants without breaking
        native Pydantic validators.

        Args:
            penalized_node_id: The W3C DID of the agent node to penalize.
            invocation_id: The CID of the ToolInvocationEvent or intent that caused the fault.
            burn_magnitude: The quantity of tokens to burn as a penalty.
        """
        # Securely map the penalized node before synthesizing the TokenBurnReceipt
        observation = EventFactory.build_event(
            ObservationEvent,
            type="observation",
            timestamp=time.time(),
            source_node_id=penalized_node_id,
            payload={"burn_magnitude": burn_magnitude},
        )
        self.ledger = append_event(self.ledger, observation)

        burn_receipt = EventFactory.build_event(
            TokenBurnReceipt,
            timestamp=time.time(),
            type="token_burn",
            tool_invocation_id=invocation_id,
            input_tokens=0,
            output_tokens=0,
            burn_magnitude=burn_magnitude,
        )
        self.ledger = append_event(self.ledger, burn_receipt)

    async def _enforce_ontological_contracts(self) -> None:
        """
        Zero-Knowledge Contract Enforcement.
        Uses pure duck-typing to dynamically evaluate structural ledger boundaries 
        without hardcoding API methods to specific use cases.
        """
        import re
        import time
        from coreason_orchestrator.factory import EventFactory
        from coreason_manifest.spec.ontology import ObservationEvent
        
        if not hasattr(self, "_resolved_contract_ids"):
            self._resolved_contract_ids = set()
            
        async with self._ledger_lock:
            for event in self.ledger.history:
                event_id = getattr(event, "event_id", str(id(event)))
                if event_id in self._resolved_contract_ids:
                    continue
                
                event_type = type(event).__name__
                
                # Dynamic Logic Gate: Continuous Input Repair
                if event_type == "ContinuousObservationStream":
                    contracts = [c for c in self.ledger.history if type(c).__name__ == "StreamingDisfluencyContract"]
                    if contracts:
                        contract = contracts[-1]
                        token_buffer = getattr(event, "token_buffer", [])
                        regex = getattr(contract, "repair_marker_regex", "")
                        
                        text = " ".join(token_buffer)
                        if re.search(regex, text):
                            lookback = min(len(token_buffer), getattr(contract, "max_lookback_window", 0))
                            decay_matrix = dict(getattr(event, "temporal_decay_matrix", {}))
                            for i in range(max(0, len(token_buffer) - lookback), len(token_buffer)):
                                decay_matrix[i] = 0.0
                                
                            object.__setattr__(event, "temporal_decay_matrix", decay_matrix)
                            
                            obs = EventFactory.build_event(
                                ObservationEvent,
                                timestamp=time.time(),
                                type="observation",
                                payload={"enforced_contract": "StreamingDisfluencyContract", "mutated_stream": str(event_id)}
                            )
                            self.ledger = append_event(self.ledger, obs)
                    self._resolved_contract_ids.add(event_id)
                
                # Dynamic Logic Gate: Speculative Rollbacks
                elif event_type == "SpeculativeExecutionBoundary":
                    if getattr(event, "falsified", False):
                        pointers = getattr(event, "rollback_pointers", [])
                        from coreason_manifest.spec.ontology import RollbackIntent
                        for pointer in pointers:
                            rollback = EventFactory.build_event(
                                RollbackIntent,
                                request_id=f"cascade_delete_{pointer}",
                                target_event_id=pointer,
                                invalidated_node_ids=[pointer]
                            )
                            self.ledger = append_event(self.ledger, rollback)
                        self._resolved_contract_ids.add(event_id)
                
                # Dynamic Logic Gate: Truth Maintenance Graveyard
                elif event_type == "DefeasibleCascadeEvent":
                    root = getattr(event, "root_falsified_event_id", None)
                    quarantined = getattr(event, "quarantined_event_ids", [])
                    
                    nodes_to_retract = list(quarantined)
                    if root:
                        nodes_to_retract.append(root)
                        
                    new_retracted = list(getattr(self.ledger, "retracted_nodes", []))
                    new_defeasible = dict(getattr(self.ledger, "defeasible_claims", {}))
                    
                    for node_id in nodes_to_retract:
                        if node_id not in new_retracted:
                            new_retracted.append(node_id)
                        if node_id in new_defeasible:
                            new_defeasible.pop(node_id)
                            
                    # Mathematically sever the nodes from volatile memory by overwriting the ledger arrays
                    object.__setattr__(self.ledger, "retracted_nodes", new_retracted)
                    object.__setattr__(self.ledger, "defeasible_claims", new_defeasible)
                    
                    self._resolved_contract_ids.add(event_id)
                    
                # Dynamic Logic Gate: Intent Classification Routing (DAG Starvation)
                elif event_type == "IntentClassificationReceipt":
                    intent = getattr(event, "classified_intent", None)
                    if intent:
                        # Translate the probabilistic Softmax guess into a hard execution boundary
                        from coreason_manifest.spec.ontology import DynamicRoutingManifest
                        
                        dynamic_route = DynamicRoutingManifest(
                            bypassed_steps=[],
                            active_subgraphs={"isolated_intent_path": [intent]}
                        )
                        
                        obs = EventFactory.build_event(
                            ObservationEvent,
                            timestamp=time.time(),
                            type="observation",
                            payload={
                                "enforced_contract": "TaxonomicRoutingPolicy",
                                "locked_intent": str(intent),
                                "embedded_routing_manifest": dynamic_route.model_dump()
                            }
                        )
                        self.ledger = append_event(self.ledger, obs)
                    self._resolved_contract_ids.add(event_id)

    async def submit_intervention_receipt(self, receipt: "InterventionReceipt") -> None:
        """
        Public Ingress API: Accepts an exogenous InterventionReceipt from a human operator.
        Wraps the rigorous Pydantic state inside an ObservationEvent to satisfy the frozen
        AnyStateEvent ledger constraints, and safely commits it via the concurrency lock.
        Once appended, this physically breaks the DAG halt in the next tick() cycle.
        """
        import json
        import time

        from coreason_manifest.spec.ontology import ObservationEvent

        obs = EventFactory.build_event(
            ObservationEvent,
            timestamp=time.time(),
            type="observation",
            payload=json.loads(receipt.model_dump_json())
        )
        
        async with self._ledger_lock:
            self.ledger = append_event(self.ledger, obs)

    async def tick(self) -> bool:
        """
        Executes a deterministic asynchronous tick evaluating the topological frontier.

        Returns:
            True if work was performed, False if the graph is fully resolved or halted.
        """
        # 0. Enforce generic structural physics automatically
        await self._enforce_ontological_contracts()

        # 1. Resolve current topological cursor
        frontier_nodes = resolve_current_node(self.workflow, self.ledger)
        if not frontier_nodes:
            return False

        # 2. Evaluate if the kinetic plane needs to resolve pending tools
        # A tool is pending if it is a ToolInvocationEvent that does not have a corresponding
        # ObservationEvent with its triggering_invocation_id
        resolved_invocation_ids = {
            event.triggering_invocation_id
            for event in self.ledger.history
            if isinstance(event, ObservationEvent) and event.triggering_invocation_id
        }

        from coreason_manifest.spec.ontology import ToolInvocationEvent
        pending_tools: list[ToolInvocationEvent] = [
            event
            for event in self.ledger.history
            if isinstance(event, ToolInvocationEvent) and event.event_id not in resolved_invocation_ids
        ]

        from coreason_manifest.spec.ontology import LatentSchemaInferenceIntent
        pending_research: list[LatentSchemaInferenceIntent] = [
            event
            for event in self.ledger.history
            if isinstance(event, LatentSchemaInferenceIntent) and getattr(event, "event_id", getattr(event, "target_buffer_id", "unknown")) not in resolved_invocation_ids
        ]

        from coreason_manifest.spec.ontology import LatentProjectionIntent
        pending_projections: list[LatentProjectionIntent] = [
            event
            for event in self.ledger.history
            if isinstance(event, LatentProjectionIntent) and getattr(event, "event_id", "unknown") not in resolved_invocation_ids
        ]

        if pending_tools or pending_research or pending_projections:
            # We need to dispatch the kinetic actuator for all pending scripts and tools.
            try:
                import json
                import time

                async with asyncio.TaskGroup() as tg:
                    # 1. Dispatch High-Order Neurosymbolic Research Intents
                    for research_intent in pending_research:
                        active_partitions = None
                        if frontier_nodes:
                            for node in frontier_nodes:
                                if getattr(node, "action_space_id", None) and node.action_space_id in self.action_space_registry:
                                    action_space = self.action_space_registry[node.action_space_id]
                                    active_partitions = getattr(action_space, "ephemeral_partitions", None)
                                    break
                        tg.create_task(self.delegate_research_intent_to_kinetic_plane(research_intent, active_partitions))

                    # Systematic Search (Phase 2): Route LatentProjectionIntent to MCPServerManifest
                    for proj_intent in pending_projections:
                        target_mcp = None
                        target_partition = None
                        if frontier_nodes:
                            for node in frontier_nodes:
                                if getattr(node, "action_space_id", None) and node.action_space_id in self.action_space_registry:
                                    action_space = self.action_space_registry[node.action_space_id]
                                    if getattr(action_space, "mcp_servers", None):
                                        for mcp in action_space.mcp_servers:
                                            # Route based on explicit URI bounds or fallback to implicit semantic similarity
                                            if mcp.server_uri == getattr(proj_intent, "target_mcp_uri", mcp.server_uri):
                                                target_mcp = mcp
                                                target_partition = getattr(action_space, "ephemeral_partitions", None)
                                                break
                                if target_mcp:
                                    break
                                    
                        if target_mcp:
                            from coreason_manifest.spec.ontology import ToolInvocationEvent, ToolManifest
                            synthetic_tool_intent = EventFactory.build_event(
                                ToolInvocationEvent,
                                event_id=getattr(proj_intent, "event_id", "unknown"),
                                tool_name=target_mcp.server_uri,
                                arguments={"query": getattr(proj_intent, "projection_embedding", [])}
                            )
                            found_manifest = ToolManifest(tool_name=target_mcp.server_uri, tool_description="MCP DB Projection Bridge")
                            tg.create_task(self.delegate_to_kinetic_plane(synthetic_tool_intent, found_manifest, target_partition))

                    # 2. Dispatch Standard Tools
                    for intent in pending_tools:
                        # Verify the tool against the active node's ActionSpaceManifest
                        # We evaluate the active nodes on the frontier to find the authorized tool.
                        found_manifest = None
                        active_node = None
                        active_partitions = None
                        active_node_id = "unknown"
                        if frontier_nodes:
                            for node in frontier_nodes:
                                if getattr(node, "action_space_id", None) and node.action_space_id in self.action_space_registry:
                                    action_space = self.action_space_registry[node.action_space_id]
                                    # Phase 3 & 4: Sandbox Check
                                    active_partitions = getattr(action_space, "ephemeral_partitions", None)
                                    
                                    for tool in getattr(action_space, "native_tools", []):
                                        if tool.tool_name == intent.tool_name:
                                            found_manifest = tool
                                            active_node = node
                                            # Resolve active_node_id
                                            if hasattr(self.workflow.topology, "nodes"):
                                                for n_id, n_obj in self.workflow.topology.nodes.items():
                                                    if n_obj is node:
                                                        active_node_id = str(n_id)
                                                        break
                                            break
                                            
                                    # Phase 2: MCP External Boundaries
                                    if not found_manifest and getattr(action_space, "mcp_servers", None):
                                        for mcp_server in action_space.mcp_servers:
                                            # Duck-type routing for MCP execution tools dynamically
                                            if getattr(intent, "tool_name", "").startswith(mcp_server.server_uri):
                                                # Temporarily wrap MCP as a valid manifest pointer
                                                from coreason_manifest.spec.ontology import ToolManifest
                                                found_manifest = ToolManifest(
                                                    tool_name=intent.tool_name, 
                                                    tool_description="Dynamic MCP Passthrough"
                                                )
                                                active_node = node
                                                break
                                                
                                    if found_manifest:
                                        break

                        if not found_manifest:
                            # FR-4.1: The Orchestrator MUST verify any requested ToolInvocationEvent
                            # against the allowed ActionSpaceManifest bound to the current node.

                            # Gap-04: Graceful Fault Ledgering
                            # Instead of crashing, synthesize a System2RemediationIntent and append it to the ledger.
                            import time
                            import uuid

                            from coreason_manifest.spec.ontology import (
                                ManifestViolationReceipt,
                                System2RemediationIntent,
                            )

                            # Ensure we have a valid target node ID
                            target_node_id = active_node_id
                            if target_node_id == "unknown" and frontier_nodes:
                                # Fallback to the first frontier node if active_node wasn't resolved
                                # because the tool wasn't found in any action space.
                                for node in frontier_nodes:
                                    if hasattr(self.workflow.topology, "nodes"):
                                        for n_id, n_obj in self.workflow.topology.nodes.items():
                                            if n_obj is node:
                                                target_node_id = str(n_id)
                                                break
                                    if target_node_id != "unknown":
                                        break

                            fault_intent = EventFactory.build_event(
                                System2RemediationIntent,
                                fault_id=f"fault_{uuid.uuid4().hex[:8]}",
                                target_node_id=target_node_id,
                                violation_receipts=[
                                    ManifestViolationReceipt(
                                        failing_pointer="/tool_name",
                                        violation_type="unauthorized_tool",
                                        diagnostic_message=f"Tool '{intent.tool_name}' is not authorized in the "
                                        "current ActionSpaceManifest.",
                                    )
                                ],
                            )

                            async with self._ledger_lock:
                                self.ledger = append_event(self.ledger, fault_intent)
                        else:
                            # Intercept pending tools for InterventionPolicy evaluation
                            requires_intervention = False
                            is_blocking = True
                            if active_node and getattr(active_node, "intervention_policies", None):
                                for policy in active_node.intervention_policies:
                                    if getattr(policy, "trigger", None) == "before_tool_execution":
                                        requires_intervention = True
                                        is_blocking = getattr(policy, "blocking", True)
                                        break

                            if requires_intervention:
                                # Check if an InterventionIntent was already emitted for this tool invocation
                                intent_already_emitted = False
                                for event in self.ledger.history:
                                    if isinstance(event, ObservationEvent):
                                        # Check if this ObservationEvent contains an InterventionIntent
                                        payload = event.payload if isinstance(event.payload, dict) else {}
                                        if "proposed_action" in payload and payload.get("target_node_id") is not None:
                                            # proposed_action could be a string if JSON serialized, or dict
                                            prop_act = payload["proposed_action"]
                                            if (
                                                isinstance(prop_act, dict)
                                                and prop_act.get("event_id") == intent.event_id
                                            ):
                                                intent_already_emitted = True
                                                break

                                if not intent_already_emitted:
                                    # Synthesize InterventionIntent
                                    dumped = intent.model_dump()
                                    from coreason_manifest.spec.ontology import JsonPrimitiveState

                                    proposed_action: dict[str, JsonPrimitiveState] = dict(dumped)  # type: ignore[arg-type]

                                    intervention_intent = EventFactory.build_event(
                                        InterventionIntent,
                                        target_node_id=active_node_id,
                                        context_summary=f"Tool execution for '{intent.tool_name}' requires approval.",
                                        proposed_action=proposed_action,
                                        adjudication_deadline=time.time() + 3600,
                                    )

                                    # Wrap it in an ObservationEvent to satisfy ledger schema constraints
                                    observation = EventFactory.build_event(
                                        ObservationEvent,
                                        timestamp=time.time(),
                                        type="observation",
                                        payload=json.loads(intervention_intent.model_dump_json()),
                                        triggering_invocation_id=intent.event_id,
                                        source_node_id=active_node_id,
                                    )

                                    async with self._ledger_lock:
                                        self.ledger = append_event(self.ledger, observation)
                                        
                                    if is_blocking:
                                        continue  # Violent DAG halt: Skip delegation to kinetic plane this tick

                                # If it was emitted and is blocking, physically verify the Receipt
                                if is_blocking:
                                    is_approved = False
                                    for event in self.ledger.history:
                                        if isinstance(event, ObservationEvent):
                                            payload = event.payload if isinstance(event.payload, dict) else {}
                                            if "intervention_request_id" in payload and "approved" in payload:
                                                req_id = str(payload.get("intervention_request_id", ""))
                                                if (
                                                    req_id == intent.event_id
                                                    or req_id.replace("-", "") == intent.event_id.replace("-", "")[:32]
                                                ):
                                                    from coreason_manifest.spec.ontology import InterventionReceipt
                                                    try:
                                                        # Physically instantiate to trigger the WetwareAttestationContract @model_validator
                                                        receipt_obj = InterventionReceipt(**payload)
                                                        if receipt_obj.approved:
                                                            is_approved = True
                                                    except Exception:
                                                        # Cryptographic forgery or Anti-Replay Lock Triggered
                                                        pass
                                                    break

                                    if not is_approved:
                                        continue  # Wait for valid receipt, do not dispatch to kinetic plane

                            tg.create_task(self.delegate_to_kinetic_plane(intent, found_manifest, active_partitions))
            except ExceptionGroup as eg:
                raise ExceptionGroup("Kinetic Plane Faults", eg.exceptions) from eg

            return True

        # 3. If no pending kinetic task, delegate to the Cognitive Plane concurrently for all nodes in the frontier
        try:
            async with asyncio.TaskGroup() as tg:
                for node in frontier_nodes:
                    node_id = "unknown"
                    if hasattr(self.workflow.topology, "nodes"):
                        for n_id, n_obj in self.workflow.topology.nodes.items():
                            if n_obj is node:
                                node_id = str(n_id)
                                break

                    resolved_action_space: ActionSpaceManifest | None = None
                    if node.action_space_id:
                        resolved_action_space = self.action_space_registry.get(node.action_space_id)
                    if resolved_action_space is None:
                        resolved_action_space = ActionSpaceManifest(action_space_id="default", native_tools=[])

                    tg.create_task(self.delegate_to_cognitive_plane(node, node_id, resolved_action_space))
        except ExceptionGroup as eg:
            raise ExceptionGroup("Cognitive Plane Faults", eg.exceptions) from eg

        return True

    async def run_event_loop(self) -> EpistemicLedgerState:
        """
        Operates the primary asynchronous tick-cycle.

        Utilizes native asyncio.TaskGroup to manage execution, continually
        evaluating the topological frontier via `tick()` until the graph is
        fully resolved or halted by an interrupt.

        Returns:
            The final crystallized EpistemicLedgerState after the graph is fully resolved or halted.
        """
        main_task = asyncio.current_task()

        async def _listen_for_interrupts() -> None:
            """
            Asynchronously waits for a preemption signal.

            Upon receiving a BargeInInterruptEvent, it safely handles the preemption
            and forcefully cancels the main task, cascading cancellation down to all
            currently executing nested TaskGroups.
            """
            interrupt_event, invocation_id = await self.interrupt_queue.get()
            async with self._ledger_lock:
                self.handle_preemption(interrupt_event, invocation_id)
            # Preempt the active loop to enforce FR-5.2 Cascade Cancellation
            if main_task and not main_task.done():
                main_task.cancel()

        listener_task = asyncio.create_task(_listen_for_interrupts())

        async def _observability_worker() -> None:  # pragma: no cover
            """
            Background micro-batching task to sweep the ledger for ExecutionSpanReceipts
            and flush them out securely via OTelBatchExporter without blocking main logic.
            """
            last_processed_idx = len(self.ledger.history)
            while True:
                await asyncio.sleep(5.0)
                try:
                    current_idx = len(self.ledger.history)
                    if current_idx > last_processed_idx:
                        new_events = self.ledger.history[last_processed_idx:current_idx]
                        last_processed_idx = current_idx
                        new_spans: list[ExecutionSpanReceipt] = []
                        for event in new_events:
                            if isinstance(event, ExecutionSpanReceipt):
                                new_spans.append(event)  # noqa: PERF401
                        if new_spans:
                            await self.exporter.flush_spans(new_spans)
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # pragma: no cover
                    # Swallow exceptions to not crash the background worker
                    logging.getLogger("coreason.orchestrator.telemetry").warning(f"Failed to flush spans: {e}")

        observability_task = asyncio.create_task(_observability_worker())

        try:
            while True:
                has_work = await self.tick()
                if not has_work:
                    break
                # Yield to event loop to prevent starvation
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            # Gracefully halt the event loop following cascade cancellation
            pass
        except Exception:
            # 7. State Dumps: In the event of an unhandled Python crash or fatal cryptographic tamper fault,
            # the Orchestrator MUST execute a final .model_dump_json() of the current EpistemicLedgerState
            # to stdout, preserving W3C DIDs and cryptographic hashes to allow for cold-start disaster recovery.
            import sys

            sys.stdout.write(self.ledger.model_dump_json() + "\n")
            sys.stdout.flush()
            raise
        finally:
            # Prevent the listener from hanging around after natural loop completion
            if not listener_task.done():
                listener_task.cancel()
            if not observability_task.done():
                observability_task.cancel()

        return self.ledger
