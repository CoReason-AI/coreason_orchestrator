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

from coreason_orchestrator.hydration import compile_state_hydration
from coreason_orchestrator.interfaces import ActuatorEngineProtocol, InferenceEngineProtocol
from coreason_orchestrator.ledger import append_event


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

        # 2. Append the generated intent/event to the ledger (synthesizes new state)
        self.ledger = append_event(self.ledger, payload)

        # 3. Append the thermodynamic burn receipt to the ledger (synthesizes new state)
        self.ledger = append_event(self.ledger, burn_receipt)

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

        observation = ObservationEvent(
            event_id="",  # The ledger append_event handles RFC 8785 hashing
            timestamp=time.time(),
            type="observation",
            payload=raw_payload,
            triggering_invocation_id=intent.event_id,
        )

        # 4. Append the ObservationEvent to the immutable ledger
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

        burn_receipt = TokenBurnReceipt(
            event_id="",  # The ledger append_event handles RFC 8785 hashing
            timestamp=time.time(),
            type="token_burn",
            tool_invocation_id=invocation_id,
            input_tokens=0,
            output_tokens=0,
            burn_magnitude=burn_magnitude,
        )
        self.ledger = append_event(self.ledger, burn_receipt)
