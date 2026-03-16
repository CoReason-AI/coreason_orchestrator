# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from coreason_manifest.spec.ontology import (
    DAGTopologyManifest,
    EpistemicProvenanceReceipt,
    WorkflowManifest,
)
from typer.testing import CliRunner

from coreason_orchestrator.main import DummyActuatorEngine, DummyInferenceEngine, app

runner = CliRunner()


def get_mock_workflow() -> WorkflowManifest:
    topology = DAGTopologyManifest(max_depth=5, edges=[], nodes={}, max_fan_out=5, lifecycle_phase="draft")
    return WorkflowManifest(
        manifest_version="1.0.0",
        topology=topology,
        genesis_provenance=EpistemicProvenanceReceipt(extracted_by="did:coreason:sys", source_event_id="dummy"),
    )


def test_cli_invalid_file() -> None:
    result = runner.invoke(app, ["non_existent_file.json"])
    assert result.exit_code == 1


def test_cli_success(tmp_path: Path) -> None:
    workflow = get_mock_workflow()
    workflow_path = tmp_path / "workflow.json"
    workflow_path.write_text(workflow.model_dump_json())

    # We patch CoreOrchestrator to avoid running the real logic or stubs failing
    with patch("coreason_orchestrator.main.CoreOrchestrator") as mock_orchestrator:
        mock_instance = mock_orchestrator.return_value
        mock_instance.run_event_loop = AsyncMock()

        result = runner.invoke(app, [str(workflow_path)])
        assert result.exit_code == 0
        mock_instance.run_event_loop.assert_called_once()


def test_cli_success_yaml(tmp_path: Path) -> None:
    import yaml

    workflow = get_mock_workflow()
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.dump(json.loads(workflow.model_dump_json())))

    with patch("coreason_orchestrator.main.CoreOrchestrator") as mock_orchestrator:
        mock_instance = mock_orchestrator.return_value
        mock_instance.run_event_loop = AsyncMock()

        result = runner.invoke(app, [str(workflow_path)])
        assert result.exit_code == 0
        mock_instance.run_event_loop.assert_called_once()


def test_cli_disaster_recovery(tmp_path: Path) -> None:
    workflow = get_mock_workflow()
    workflow_path = tmp_path / "workflow.json"
    workflow_path.write_text(workflow.model_dump_json())

    with patch("coreason_orchestrator.main.CoreOrchestrator") as mock_orchestrator:
        mock_instance = mock_orchestrator.return_value
        mock_instance.run_event_loop = AsyncMock(side_effect=RuntimeError("Simulated fault"))

        with patch("coreason_orchestrator.main.Path.write_text") as mock_write_text:
            # Runner captures exceptions, but we re-raise `e` in main.py, so it might fail
            result = runner.invoke(app, [str(workflow_path)])
            assert result.exit_code != 0
            assert isinstance(result.exception, RuntimeError)
            assert str(result.exception) == "Simulated fault"

            mock_write_text.assert_called_once()
            assert "history" in mock_write_text.call_args[0][0]


@pytest.mark.asyncio
async def test_dummy_inference_engine() -> None:
    engine = DummyInferenceEngine()
    with pytest.raises(NotImplementedError):
        await engine.generate_intent(None, None)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_dummy_actuator_engine() -> None:
    engine = DummyActuatorEngine()
    with pytest.raises(NotImplementedError):
        await engine.execute(None, None, None)  # type: ignore[arg-type]
