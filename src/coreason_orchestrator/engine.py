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

from coreason_manifest.spec.ontology import (
    ActionSpaceManifest,
    AgentNodeProfile,
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

        Strictly awaits a mathematically validated intent or state event along with its
        thermodynamic TokenBurnReceipt, appending both as immutable differentials
        to the epistemic ledger.

        Args:
            node: The current active AgentNodeProfile requesting cognitive evaluation.
            node_id: The identifier of the node.
            action_space: The action space manifest.
        """
        # 1. Dispatch the current node profile and strictly read-only ledger
        payload, burn_receipt, _scratchpad, _cognitive_receipt = await self.inference_engine.generate_intent(
            node, self.ledger, node_id, action_space
        )

        # 2. Append the generated intent/event and thermodynamic burn receipt to the ledger securely
        async with self._ledger_lock:
            new_ledger = append_event(self.ledger, payload)
            self.ledger = append_event(new_ledger, burn_receipt)

    async def delegate_to_kinetic_plane(self, intent: ToolInvocationEvent, manifest: ToolManifest) -> None:
        """
        Delegates the execution of a tool to the air-gapped Kinetic Plane.

        To enforce absolute Zero-Trust, the Orchestrator passes a compiled StateHydrationManifest
        and strictly awaits a raw JsonPrimitiveState payload. It natively synthesizes the
        ObservationEvent and securely binds the triggering_invocation_id before appending.

        Args:
            intent: The validated ToolInvocationEvent from the Cognitive Plane.
            manifest: The associated ToolManifest defining the capability.
        """
        # 2. Dispatch execution and strictly await the raw JSON payload
        raw_payload = await self.actuator_engine.execute(intent, manifest, self.ledger.eviction_policy)

        # 3. Securely synthesize the ObservationEvent natively (Zero-Trust)
        # We must explicitly cast raw_payload to dict[str, Any] as required by ObservationEvent payload
        if not isinstance(raw_payload, dict):
            raw_payload = {"result": raw_payload}

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

    async def tick(self) -> bool:
        """
        Executes a deterministic asynchronous tick evaluating the topological frontier.

        Returns:
            True if work was performed, False if the graph is fully resolved or halted.
        """
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

        pending_tools: list[ToolInvocationEvent] = [
            event
            for event in self.ledger.history
            if isinstance(event, ToolInvocationEvent) and event.event_id not in resolved_invocation_ids
        ]

        if pending_tools:
            # We need to dispatch the kinetic actuator for all pending tools.
            try:
                import json
                import time

                async with asyncio.TaskGroup() as tg:
                    for intent in pending_tools:
                        # Verify the tool against the active node's ActionSpaceManifest
                        # We evaluate the active nodes on the frontier to find the authorized tool.
                        found_manifest = None
                        active_node = None
                        active_node_id = "unknown"
                        if frontier_nodes:
                            for node in frontier_nodes:
                                if node.action_space_id and node.action_space_id in self.action_space_registry:
                                    action_space = self.action_space_registry[node.action_space_id]
                                    for tool in action_space.native_tools:
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
                                    if found_manifest:
                                        break

                        if not found_manifest:
                            # FR-4.1: The Orchestrator MUST verify any requested ToolInvocationEvent
                            # against the allowed ActionSpaceManifest bound to the current node.

                            # Gap-04: Graceful Fault Ledgering
                            # Instead of crashing, synthesize a System2RemediationIntent and append it to the ledger.
                            import time
                            import uuid

                            from coreason_manifest.spec.ontology import System2RemediationIntent

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
                                failing_pointers=["/tool_name"],
                                remediation_prompt=f"Tool '{intent.tool_name}' is not authorized in the "
                                "current ActionSpaceManifest.",
                            )

                            async with self._ledger_lock:
                                self.ledger = append_event(self.ledger, fault_intent)  # type: ignore[arg-type]
                        else:
                            # Intercept pending tools for InterventionPolicy evaluation
                            requires_intervention = False
                            if active_node and active_node.intervention_policies:
                                for policy in active_node.intervention_policies:
                                    if policy.trigger == "before_tool_execution":
                                        requires_intervention = True
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
                                    proposed_action: dict[str, str | int | float | bool | None] = {}
                                    for k, v in dumped.items():
                                        if isinstance(v, (str, int, float, bool, type(None))):
                                            proposed_action[k] = v
                                        else:
                                            proposed_action[k] = json.dumps(v)

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
                                    continue  # Skip delegation to kinetic plane this tick

                                # If it was emitted, check if there is an approved InterventionReceipt
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
                                                if payload.get("approved"):
                                                    is_approved = True
                                                break

                                if not is_approved:
                                    continue  # Wait for receipt, do not dispatch to kinetic plane

                            tg.create_task(self.delegate_to_kinetic_plane(intent, found_manifest))
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
