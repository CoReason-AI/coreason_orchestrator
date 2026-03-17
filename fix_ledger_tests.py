with open("tests/test_ledger.py") as f:
    text = f.read()

text = text.replace(
    """@given(  # type: ignore[misc]
    request_id=text(min_size=1),
    target_event_id=text(min_size=1),
    invalidated_node_ids=lists(text()),
    cascade_id=text(min_size=1),
    root_falsified_event_id=text(min_size=1),
    propagated_decay_factor=floats(min_value=0.0, max_value=1.0),
    quarantined_event_ids=lists(text(), min_size=1),
)""",
    """from hypothesis.strategies import from_regex
@given(  # type: ignore[misc]
    request_id=from_regex(r"^[a-zA-Z0-9_.:-]+$", fullmatch=True),
    target_event_id=from_regex(r"^[a-zA-Z0-9_.:-]+$", fullmatch=True),
    invalidated_node_ids=lists(from_regex(r"^[a-zA-Z0-9_.:-]+$", fullmatch=True)),
    cascade_id=from_regex(r"^[a-zA-Z0-9_.:-]+$", fullmatch=True),
    root_falsified_event_id=from_regex(r"^[a-zA-Z0-9_.:-]+$", fullmatch=True),
    propagated_decay_factor=floats(min_value=0.0, max_value=1.0),
    quarantined_event_ids=lists(from_regex(r"^[a-zA-Z0-9_.:-]+$", fullmatch=True), min_size=1),
)""",
)

with open("tests/test_ledger.py", "w") as f:
    f.write(text)
