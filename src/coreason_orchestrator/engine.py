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
    EpistemicLedgerState,
    WorkflowManifest,
)

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
