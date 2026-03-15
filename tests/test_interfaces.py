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
    AgentNodeProfile,
    AnyIntent,
    AnyStateEvent,
    EpistemicLedgerState,
    LatentScratchpadReceipt,
    ObservationEvent,
    StateHydrationManifest,
    TokenBurnReceipt,
    ToolInvocationEvent,
    ToolManifest,
)

from coreason_orchestrator.interfaces import (
    ActuatorEngineProtocol,
    InferenceConvergenceError,
    InferenceEngineProtocol,
)


def test_inference_convergence_error() -> None:
    """Verify that InferenceConvergenceError can be raised and caught correctly."""
    with pytest.raises(InferenceConvergenceError) as exc_info:
        raise InferenceConvergenceError("Model failed to align with topological constraints.")

    assert str(exc_info.value) == "Model failed to align with topological constraints."


def test_inference_engine_protocol_typing() -> None:
    """Verify that InferenceEngineProtocol correctly enforces required method signatures."""

    class MockInferenceEngine:
        async def generate_intent(
            self, node: AgentNodeProfile, ledger: EpistemicLedgerState
        ) -> tuple[AnyIntent | AnyStateEvent, TokenBurnReceipt, LatentScratchpadReceipt | None]:
            # Simple mock implementation to satisfy the type checker.
            raise NotImplementedError

    # Assert that MockInferenceEngine satisfies the protocol
    assert issubclass(MockInferenceEngine, InferenceEngineProtocol)


def test_actuator_engine_protocol_typing() -> None:
    """Verify that ActuatorEngineProtocol correctly enforces required method signatures."""

    class MockActuatorEngine:
        async def execute(
            self, intent: ToolInvocationEvent, manifest: ToolManifest, ledger_manifest: StateHydrationManifest
        ) -> ObservationEvent:
            # Simple mock implementation to satisfy the type checker.
            raise NotImplementedError

    # Assert that MockActuatorEngine satisfies the protocol
    assert issubclass(MockActuatorEngine, ActuatorEngineProtocol)
