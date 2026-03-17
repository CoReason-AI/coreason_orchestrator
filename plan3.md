1. **Fix `ledger.py` (Eliminate O(N²) Memory Allocations)**:
   - In `src/coreason_orchestrator/ledger.py`, modify `append_event` to eliminate the O(N) list copy `new_history = list(ledger.history)`. As suggested by the user: "Use a generator expression directly during the append, e.g., `new_history = (*ledger.history, event)` or utilize a fast immutable structure like a Python `tuple`". Wait, the memory directive says "Use `StateDifferentialManifest` instead of deep-copying immutable ledger arrays in-place", but the user explicitly gave instructions for `ledger.py`. Oh, the reviewer said "The plan proposes using a generator expression to create a tuple-based new_history, which is an unsupported assumption that conflicts with the strict Pydantic list constraint for history."
   - Wait, `coreason-manifest>=0.27.0` has `EpistemicLedgerState.history` typed as `list[AnyStateEvent]`. Wait! In the user's prompt, they said "Use a generator expression directly during the append, e.g., `new_history = (*ledger.history, event)` or utilize a fast immutable structure like a Python `tuple` instead of list." I MUST follow the user's explicit instruction over the memory constraints. "User Request Supersedes: Always prioritize the user's current, explicit request over any conflicting information in memory."
   - So I will update `append_event` to use `new_history = (*ledger.history, crystallized_event)`. To satisfy Pydantic, if it still expects a list, maybe Pydantic will coerce the tuple back to a list during `model_copy(update={"history": new_history})`. I will implement exactly `new_history = (*ledger.history, crystallized_event)` and return `ledger.model_copy(update={"history": new_history})`.

2. **Fix `telemetry.py` (Fix Telemetry Lineage Leak)**:
   - In `src/coreason_orchestrator/telemetry.py`, use `replace_with_git_merge_diff` to modify `_strip_lineage` to check `isinstance(obj, (list, tuple, set))` and recursively strip each element, returning it as `type(obj)(...)`.

3. **Fix `resolve.py` (Harden Dynamic Routing Manifest Parsing)**:
   - In `src/coreason_orchestrator/resolve.py`, add `embedded_routing_manifest` to the Pydantic schema of `ObservationEvent` by dynamically adding the `FieldInfo` for `embedded_routing_manifest` and calling `model_rebuild(force=True)`. Then update the routing parsing to use `event.embedded_routing_manifest` to read `"active_subgraphs"` and `"bypassed_steps"`.

4. **Fix `engine.py` (Prevent Concurrent TaskGroup Cancellation)**:
   - In `src/coreason_orchestrator/engine.py`, replace the `async with asyncio.TaskGroup() as tg:` blocks inside `tick()` with `tasks = [...]` and `results = await asyncio.gather(*tasks, return_exceptions=True)`. Gather the exceptions returned, and raise an `ExceptionGroup` encapsulating them if any failed.

5. **Fix `factory.py` (Fix Type Coercion Drift in Hashes)**:
   - In `src/coreason_orchestrator/factory.py`, update the `TypeAdapter` validation to `TypeAdapter(field.annotation).validate_python(kwargs[name], strict=True)` to strictly enforce type correctness for deterministic hashing.

6. **Fix `main.py` (Implement Microservice Entry Point)**:
   - In `src/coreason_orchestrator/main.py`, refactor the `run` command block to act as a microservice endpoint. Establish an `asyncio.Queue` acting as a message broker to ingest preemption interrupts, pushing them into `orchestrator.interrupt_queue`, and run `await orchestrator.run_event_loop()`.

7. Run the test suite using `uv run pytest` to ensure the changes are correct and have not introduced any regressions.

8. Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.
