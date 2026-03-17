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
import json
import sys
import typing
from pathlib import Path
from typing import Any

import typer
import yaml
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
    WorkflowManifest,
)

from coreason_orchestrator.engine import CoreOrchestrator
from coreason_orchestrator.utils.logger import logger

app = typer.Typer(
    name="coreason_orchestrator",
    help="The central nervous system, epistemic router, and ledger guardian.",
    no_args_is_help=True,
)


class DummyInferenceEngine:
    async def generate_intent(
        self, node: AgentNodeProfile, ledger: EpistemicLedgerState
    ) -> tuple[AnyIntent | AnyStateEvent, TokenBurnReceipt, LatentScratchpadReceipt | None]:
        raise NotImplementedError("DummyInferenceEngine is a stub for the Cognitive Plane.")


class DummyActuatorEngine:
    async def execute(
        self, intent: ToolInvocationEvent, manifest: ToolManifest, ledger_manifest: StateHydrationManifest
    ) -> JsonPrimitiveState:
        raise NotImplementedError("DummyActuatorEngine is a stub for the Kinetic Plane.")


@app.command()  # type: ignore[misc]
def run(
    workflow_path: typing.Annotated[Path, typer.Argument(help="Path to the WorkflowManifest JSON/YAML file")],
) -> None:
    """
    Bootstraps the CoreOrchestrator and executes the primary asynchronous tick-cycle.
    """
    logger.info(f"Loading WorkflowManifest from {workflow_path}")
    if not workflow_path.exists():
        logger.error(f"Workflow file not found: {workflow_path}")
        raise typer.Abort()

    content = workflow_path.read_text()
    if workflow_path.suffix in [".yaml", ".yml"]:
        data: dict[str, Any] = yaml.safe_load(content)
    else:
        data = json.loads(content)

    workflow = WorkflowManifest.model_validate(data)
    ledger = EpistemicLedgerState(history=[])

    inference_engine = DummyInferenceEngine()
    actuator_engine = DummyActuatorEngine()

    orchestrator = CoreOrchestrator(
        workflow=workflow,
        ledger=ledger,
        inference_engine=inference_engine,  # type: ignore[arg-type]
        actuator_engine=actuator_engine,  # type: ignore[arg-type]
    )

    # 6. Bind to an actual event queue/message broker to receive preemption interrupts
    message_broker = asyncio.Queue()  # type: ignore[var-annotated]
    orchestrator.interrupt_queue = message_broker

    try:
        asyncio.run(orchestrator.run_event_loop())
    except Exception as e:
        logger.exception("A terminal fault occurred during execution.")
        dumped_ledger = ledger.model_dump_json()
        sys.stdout.write(dumped_ledger + "\n")
        sys.stdout.flush()

        recovery_path = Path("disaster_recovery_ledger.json")
        recovery_path.write_text(dumped_ledger)
        logger.info(f"Disaster recovery ledger saved to {recovery_path}")

        raise e


if __name__ == "__main__":
    app()  # pragma: no cover
