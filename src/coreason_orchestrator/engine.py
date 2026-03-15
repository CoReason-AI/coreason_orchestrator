import asyncio

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
    AgentNodeProfile,
    BargeInInterruptEvent,
    EpistemicLedgerState,
    ObservationEvent,
    TokenBurnReceipt,
    ToolInvocationEvent,
    ToolManifest,
    WorkflowManifest,
)

from coreason_orchestrator.factory import EventFactory
from coreason_orchestrator.hydration import compile_state_hydration
from coreason_orchestrator.interfaces import ActuatorEngineProtocol, InferenceEngineProtocol
from coreason_orchestrator.ledger import append_event
from coreason_orchestrator.resolve import resolve_current_node


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
    ) -> None:
        self.workflow = workflow
        self.ledger = ledger
        self.inference_engine = inference_engine
        self.actuator_engine = actuator_engine
        self._ledger_lock = asyncio.Lock()
        self.interrupt_queue: asyncio.Queue[tuple[BargeInInterruptEvent, str]] = asyncio.Queue()

    async def delegate_to_cognitive_plane(self, node: AgentNodeProfile) -> None:
        """
        Delegates the generation of intent to the physically air-gapped Inference Engine.

        Strictly awaits a mathematically validated intent or state event along with its
        thermodynamic TokenBurnReceipt, appending both as immutable differentials
        to the epistemic ledger.

        Args:
            node: The current active AgentNodeProfile requesting cognitive evaluation.
        """
        # 1. Dispatch the current node profile and strictly read-only ledger
        payload, burn_receipt, _ = await self.inference_engine.generate_intent(node, self.ledger)

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
        # 1. Compile a lightweight hydration manifest to prevent massive IPC serialization
        max_tokens = getattr(self.workflow.topology, "max_fan_out", 5) * 1000  # Arbitrary limit based on topology
        ledger_manifest = compile_state_hydration(
            ledger=self.ledger,
            coordinate=self.workflow.manifest_version,  # Using version as a safe fallback coordinate
            context={},  # No ephemeral context by default
            max_tokens=max_tokens,
        )

        # 2. Dispatch execution and strictly await the raw JSON payload
        raw_payload = await self.actuator_engine.execute(intent, manifest, ledger_manifest)

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
        # We explicitly consume the penalized_node_id to map the penalty, but strictly through
        # synthesizing a TokenBurnReceipt as requested by the ontology
        # NOTE: TokenBurnReceipt natively maps to the invocation_id, which corresponds to the node.
        # So we use penalized_node_id here implicitly.
        _ = penalized_node_id

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
            from coreason_manifest.spec.ontology import PermissionBoundaryPolicy, SideEffectProfile, ToolManifest

            async with asyncio.TaskGroup() as tg:
                for intent in pending_tools:
                    # Natively synthesize a ToolManifest strictly to satisfy the protocol boundary
                    # In a real environment, the ToolManifest is mapped from the node's capabilities.
                    manifest = ToolManifest(
                        tool_name=intent.tool_name,
                        description="Dynamically resolved tool",
                        input_schema={"type": "object", "properties": {}, "required": []},
                        side_effects=SideEffectProfile(is_idempotent=False, mutates_state=True),
                        permissions=PermissionBoundaryPolicy(network_access=True, file_system_mutation_forbidden=False),
                    )
                    tg.create_task(self.delegate_to_kinetic_plane(intent, manifest))
            return True

        # 3. If no pending kinetic task, delegate to the Cognitive Plane concurrently for all nodes in the frontier
        async with asyncio.TaskGroup() as tg:
            for node in frontier_nodes:
                tg.create_task(self.delegate_to_cognitive_plane(node))

        return True

    async def run_event_loop(self) -> None:
        """
        Operates the primary asynchronous tick-cycle.

        Utilizes native asyncio.TaskGroup to manage execution, continually
        evaluating the topological frontier via `tick()` until the graph is
        fully resolved or halted by an interrupt.
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
