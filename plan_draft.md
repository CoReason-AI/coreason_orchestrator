1. **Comprehensive Analysis & Decomposition:**
   Based on the provided BRD, FRD, and TRD, the following capabilities are currently missing or incomplete in the `CoreOrchestrator`:
   - **Atomic Unit 1:** Implement Thread-Only Telemetry Serialization using `asyncio.to_thread` (TRD 7).
   - **Atomic Unit 2:** Implement State Dumps (printing `ledger.model_dump_json()`) on fatal crash/exception inside the main event loop (TRD 7).
   - **Atomic Unit 3:** Implement an asynchronous Preemption Listener for `BargeInInterruptEvent` that triggers `asyncio.CancelledError` on the main TaskGroup (FR-5.1, FR-5.2).
   - **Atomic Unit 4:** Implement `ActionSpaceManifest` verification for `ToolInvocationEvent` before executing on the Kinetic Plane (FR-4.1).

2. **Select ONE Atomic Unit:**
   I select **Atomic Unit 2: Implement State Dumps on fatal crash/exception**. This is a small, independently testable increment that directly satisfies the observability and disaster recovery requirement from TRD 7.

3. **Implementation Plan:**
   - Modify `CoreOrchestrator.run_event_loop` to wrap the `asyncio.TaskGroup` in a `try...except Exception` block.
   - Upon catching an unhandled exception (e.g., Python crash or cryptographic fault), output `self.ledger.model_dump_json()` to `sys.stdout` and re-raise the exception.
   - Add a unit test to `tests/test_engine.py` verifying that an unhandled exception inside the event loop correctly triggers the JSON dump to stdout before propagating.
   - Complete pre commit steps to ensure proper testing, verification, review, and reflection are done.
   - Submit the change.
