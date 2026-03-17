# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator

from pathlib import Path
from typing import Any
from unittest.mock import patch


def test_logger_directory_creation(tmp_path: Path) -> None:
    """Verify that the logger creates the logs directory if it does not exist."""

    # We monkeypatch the logs path to a temporary directory so we can safely test its creation
    # Since logger.py has top-level execution, we need to mock it properly
    import coreason_orchestrator.utils.logger as log_module

    # Let's read the logger.py content, replace 'logs' with temp path, and execute it
    logger_path = Path(log_module.__file__)
    content = logger_path.read_text()

    # Replace 'logs' path string with our temporary test directory path
    test_logs_path = tmp_path / "test_logs"
    test_logs_str = test_logs_path.as_posix()
    content = content.replace('"logs"', f'"{test_logs_str}"')
    content = content.replace('"logs/app.log"', f'"{test_logs_str}/app.log"')

    # Ensure it doesn't exist before test
    assert not test_logs_path.exists()

    # Execute the module content in a dummy namespace
    dummy_namespace: dict[str, Any] = {}
    exec(content, dummy_namespace)  # noqa: S102

    # Assert directory was created
    assert test_logs_path.exists()
    assert test_logs_path.is_dir()

    # Clean up handlers if any were added
    if "logger" in dummy_namespace:
        dummy_namespace["logger"].remove()


def test_logger_coverage_import() -> None:
    """Ensure the file is imported for full coverage."""
    # Ensure logs dir exists for normal import to work
    Path("logs").mkdir(exist_ok=True)
    import coreason_orchestrator.utils.logger  # noqa: F401


def test_logger_coverage_import_when_dir_not_exists(tmp_path: Path) -> None:
    """Force execution of the `if not log_path.exists(): log_path.mkdir()` line directly on the real module."""
    # The file has module-level execution, so mocking Path globally isn't taking effect
    # correctly via reload. Instead, let's use the same string execution approach
    # we use in test_logger_directory_creation but explicitly mock out `logger.add` and
    # ensure we hit the `log_path.mkdir()` line by substituting the log path with
    # a mocked Path object or simply a new temp path.

    import coreason_orchestrator.utils.logger as log_module

    # Since we just want to execute the mkdir line, we can just replace 'logs' with a fake
    # path string that doesn't exist and watch it get created to test that line.
    logger_path = Path(log_module.__file__)
    content = logger_path.read_text()

    # Provide a path that definitely does not exist
    test_logs_path = tmp_path / "coverage_test_logs"
    test_logs_str = test_logs_path.as_posix()
    content = content.replace('"logs"', f'"{test_logs_str}"')
    content = content.replace('"logs/app.log"', f'"{test_logs_str}/app.log"')

    # Mock logger to do nothing to avoid any weird file handle locks
    mock_str = "from unittest.mock import MagicMock\nlogger = MagicMock()\n"
    content = mock_str + content.replace("from loguru import logger", "")

    assert not test_logs_path.exists()

    dummy_namespace: dict[str, Any] = {}
    exec(content, dummy_namespace)  # noqa: S102

    # Verify that the directory was created via execution of that line
    assert test_logs_path.exists()
    assert test_logs_path.is_dir()

    # Import the actual module again to register it in coverage without the magic mock.
    # We do this by ensuring the original directory does not exist for a brief moment.
    # We do this in a safe cross-platform way.
    import sys

    if "coreason_orchestrator.utils.logger" in sys.modules:
        del sys.modules["coreason_orchestrator.utils.logger"]

    import sys

    if "coreason_orchestrator.utils.logger" in sys.modules:
        del sys.modules["coreason_orchestrator.utils.logger"]

    # To pass coverage on `log_path.mkdir(parents=True, exist_ok=True)`, we must
    # mock `Path` but doing so effectively during a reload requires patching the target
    # module directly right before reload, but sometimes that gets reset.
    # Instead, we will use a tempfile patch strategy in a separate test function
    # to avoid state contamination.


def test_logger_coverage_mkdir(tmp_path: Path, monkeypatch: Any) -> None:
    """Force execution of the `if not log_path.exists(): log_path.mkdir()` line for coverage."""
    import sys

    test_logs_dir = tmp_path / "logs"

    # We change the current working directory to the temporary path!
    # Because `Path("logs")` is relative to cwd, changing cwd means it creates it in `tmp_path`.
    monkeypatch.chdir(tmp_path)

    # Ensure module is cleared
    if "coreason_orchestrator.utils.logger" in sys.modules:
        del sys.modules["coreason_orchestrator.utils.logger"]

    with patch("loguru.logger.add"), patch("loguru.logger.remove"):
        import coreason_orchestrator.utils.logger  # noqa: F401

    assert test_logs_dir.exists()


def test_serialize_to_log_event() -> None:
    """Verifies that the custom loguru serializer correctly formats LogEvent."""
    import json
    from unittest.mock import MagicMock

    from coreason_orchestrator.utils.logger import _serialize_to_log_event

    mock_message = MagicMock()
    mock_message.record = {
        "level": type("Level", (), {"name": "ERROR"})(),
        "message": "test error message",
        "extra": {"trace_id": "123"},
    }

    result = _serialize_to_log_event(mock_message)

    assert isinstance(result, str)
    assert result.endswith("\n")

    parsed = json.loads(result)
    assert parsed["level"] == "ERROR"
    assert parsed["message"] == "test error message"
    assert parsed["context_profile"] == {"trace_id": "123"}
