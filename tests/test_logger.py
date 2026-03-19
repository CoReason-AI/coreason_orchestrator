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
from unittest.mock import MagicMock, patch

import pytest


def test_logger_coverage_import() -> None:
    """Ensure the file is imported for full coverage."""
    import coreason_orchestrator.utils.logger  # noqa: F401


def test_serialize_to_log_event() -> None:
    """Verifies that the custom loguru serializer correctly formats LogEvent."""
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


@pytest.mark.asyncio
async def test_async_telemetry_sink_batching() -> None:
    """Verifies that AsyncTelemetrySink batches logs and flushes correctly."""
    from coreason_orchestrator.utils.logger import AsyncTelemetrySink

    sink = AsyncTelemetrySink(endpoint="http://mock-endpoint/v1/logs", batch_size=2)

    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_response = MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_response

        # Write first log, should not flush
        await sink.write('{"log": 1}\n')
        mock_urlopen.assert_not_called()

        # Write second log, should trigger batch flush
        await sink.write('{"log": 2}\n')

        # Allow time for async event loop tasks
        await asyncio.sleep(0.01)

        mock_urlopen.assert_called_once()
        req_arg = mock_urlopen.call_args[0][0]
        assert req_arg.full_url == "http://mock-endpoint/v1/logs"
        assert req_arg.data == b'{"log": 1}\n{"log": 2}\n'

    # Cancel the background flush task
    import contextlib

    if sink._flush_task:
        sink._flush_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sink._flush_task


@pytest.mark.asyncio
async def test_async_telemetry_sink_timer_flush() -> None:
    """Verifies that the background timer periodically flushes logs."""
    from coreason_orchestrator.utils.logger import AsyncTelemetrySink

    sink = AsyncTelemetrySink(endpoint="http://mock-endpoint/v1/logs", batch_size=10)

    with patch("urllib.request.urlopen") as mock_urlopen:
        await sink.write('{"log": "stale"}\n')

        mock_urlopen.assert_not_called()

        # Sleep enough to trigger the 1.0s background timer
        await asyncio.sleep(1.05)

        mock_urlopen.assert_called_once()
        req_arg = mock_urlopen.call_args[0][0]
        assert req_arg.data == b'{"log": "stale"}\n'

    # Cancel task
    import contextlib

    if sink._flush_task:
        sink._flush_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sink._flush_task


@pytest.mark.asyncio
async def test_async_telemetry_sink_cancellation_flush() -> None:
    """Verifies coverage when cancelling the background task."""
    from coreason_orchestrator.utils.logger import AsyncTelemetrySink

    sink = AsyncTelemetrySink(endpoint="http://mock-endpoint/v1/logs", batch_size=10)

    with patch("urllib.request.urlopen") as mock_urlopen:
        await sink.write('{"log": "cancelled"}\n')

        # Cancel the task immediately
        import contextlib

        if sink._flush_task:
            sink._flush_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sink._flush_task

        # Coverage for empty list early exit in _flush_current_batch
        sink._batch.clear()
        await sink._flush_current_batch()
        assert mock_urlopen.call_count == 0  # No additional calls


@pytest.mark.asyncio
async def test_async_telemetry_sink_failure(capsys: pytest.CaptureFixture[str]) -> None:
    """Verifies that AsyncTelemetrySink handles HTTP failures gracefully."""
    from coreason_orchestrator.utils.logger import AsyncTelemetrySink

    sink = AsyncTelemetrySink(endpoint="http://mock-endpoint/v1/logs", batch_size=1)

    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.side_effect = Exception("Connection refused")

        # The write will flush since batch_size=1
        await sink.write('{"test": "fail"}\n')

        # Wait for thread execution
        await asyncio.sleep(0.01)

        mock_urlopen.assert_called_once()

    # Exception should be printed to stderr
    captured = capsys.readouterr()
    assert "Telemetry export failed: Connection refused" in captured.err

    # Cleanup task
    import contextlib

    if sink._flush_task:
        sink._flush_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sink._flush_task
