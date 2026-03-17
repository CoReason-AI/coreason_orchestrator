1. **Fix `ledger.py` (Eliminate O(N²) Memory Allocations)**:
   - Modify `append_event` in `src/coreason_orchestrator/ledger.py` to mutate the existing history list by appending the `crystallized_event` directly, avoiding the O(N) list copy (`new_history = list(ledger.history)`). This eliminates the memory bloat.

2. **Fix `telemetry.py` (Fix Telemetry Lineage Leak)**:
   - Update `_strip_lineage` to recursively strip tuples and sets in addition to lists and dicts, ensuring no cryptographic IDs leak when tuples/sets are returned from `model_dump()`.

3. **Fix `resolve.py` (Harden Dynamic Routing Manifest Parsing)**:
   - Stop checking for `"active_subgraphs"` and `"bypassed_steps"` directly in the untyped `payload`.
   - Update `resolve_current_node` to check the `embedded_routing_manifest` field on `ObservationEvent` now that `coreason-manifest>=0.27.0` has updated the schema to include it natively. If `embedded_routing_manifest` is present, use its `active_subgraphs` and `bypassed_steps`.

4. **Fix `engine.py` (Prevent Concurrent TaskGroup Cancellation)**:
   - In `tick()`, replace the `asyncio.TaskGroup` logic with `await asyncio.gather(..., return_exceptions=True)`. Ensure sibling agents run to completion even if one raises an `InferenceConvergenceError`, and handle exceptions returned by `gather`.

5. **Fix `factory.py` (Fix Type Coercion Drift in Hashes)**:
   - In `EventFactory.build_event`, update `TypeAdapter(field.annotation).validate_python(kwargs[name])` to pass `strict=True`, forcing exact bytes and avoiding semantic coercion drift. Alternatively, bypass type coercion for hash compilation.

6. **Fix `main.py` (Implement Microservice Entry Point)**:
   - In `run()`, add logic to listen for interrupts on an `asyncio.Queue` or similar if needed by `orchestrator.interrupt_queue`. Wait, the prompt states: "It should bootstrap the CoreOrchestrator, bind to an actual event queue/message broker to receive preemption interrupts, and run the await orchestrator.run_event_loop()." I will implement a rudimentary `asyncio.Queue` and attach it to the orchestrator.

7. **Run Tests**:
   - Run the test suite using `uv run pytest` to ensure the changes are correct and have not introduced any regressions.

8. Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.
