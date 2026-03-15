# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator

import importlib
from pathlib import Path
from typing import Any


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
    content = content.replace('"logs"', f'"{test_logs_path}"')
    content = content.replace('"logs/app.log"', f'"{test_logs_path}/app.log"')

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


def test_logger_coverage_import_when_dir_not_exists() -> None:
    """Force execution of the `if not log_path.exists(): log_path.mkdir()` line directly on the real module."""
    # Temporarily hide the logs directory
    logs_dir = Path("logs")
    hidden_dir = Path(".logs_hidden")

    if logs_dir.exists():
        logs_dir.rename(hidden_dir)

    try:
        # Reloading the module should trigger directory creation since we renamed 'logs'
        import coreason_orchestrator.utils.logger as log_module

        importlib.reload(log_module)
        assert Path("logs").exists()
    finally:
        # Restore original state
        if Path("logs").exists():
            for f in Path("logs").glob("*"):
                f.unlink()
            Path("logs").rmdir()

        if hidden_dir.exists():
            hidden_dir.rename(logs_dir)
