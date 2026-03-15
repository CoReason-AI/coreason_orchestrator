# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator

from typing import Protocol, runtime_checkable

from coreason_manifest.spec.ontology import (
    AgentNodeProfile,
    AnyIntent,
    AnyStateEvent,
    EpistemicLedgerState,
    JsonPrimitiveState,
    LatentScratchpadReceipt,
    StateHydrationManifest,
    TokenBurnReceipt,
    ToolInvocationEvent,
    ToolManifest,
)


class InferenceConvergenceError(Exception):
    """Raised when the Cognitive Plane fails to converge on a valid topological structure."""


@runtime_checkable
class InferenceEngineProtocol(Protocol):
    async def generate_intent(
        self, node: AgentNodeProfile, ledger: EpistemicLedgerState
    ) -> tuple[AnyIntent | AnyStateEvent, TokenBurnReceipt, LatentScratchpadReceipt | None]:
        """
        Dispatches the read-only ledger to the air-gapped Cognitive Plane.
        The Orchestrator strictly awaits a valid intent or terminal state event
        (e.g. ToolInvocationEvent, EpistemicFlowStateReceipt) or a terminal fault.
        """


@runtime_checkable
class ActuatorEngineProtocol(Protocol):
    async def execute(
        self, intent: ToolInvocationEvent, manifest: ToolManifest, ledger_manifest: StateHydrationManifest
    ) -> JsonPrimitiveState:
        """
        Dispatches a mathematical intent to the Kinetic Plane.
        Accepts a StateHydrationManifest to prevent massive IPC serialization overhead,
        allowing the Actuator to lazy-load required state via cryptographic pointers if necessary.
        """
