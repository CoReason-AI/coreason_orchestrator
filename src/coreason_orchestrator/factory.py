# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator

from typing import Any, TypeVar

from pydantic import BaseModel

from coreason_orchestrator.utils.crypto import calculate_event_hash

T = TypeVar("T", bound=BaseModel)


class EventFactory:
    """
    An external, deterministic factory responsible for synthesizing mathematically
    bound events to resolve the Cryptographic Instantiation Paradox.
    """

    @classmethod
    def build_event(cls, event_class: type[T], **kwargs: Any) -> T:
        """
        Constructs a payload, serializes it without the event_id, computes the
        RFC 8785 JSON Control Hash, and instantiates the final Pydantic initialization.

        Args:
            event_class: The Pydantic model class to instantiate.
            **kwargs: The payload parameters for the event.

        Returns:
            A strictly instantiated event with a mathematically bound event_id.
        """
        # Identify the ID field: usually 'event_id', but 'diff_id' for StateDifferentialManifest
        id_field = "diff_id" if "diff_id" in event_class.model_fields else "event_id"

        # Pre-process arguments natively by iterating over the schema fields.
        # This properly handles Pydantic's defaults and type coercion iteratively
        # without running rigid whole-model initialization or regex validators
        # on a dummy ID, circumventing the need for create_model().
        from pydantic import TypeAdapter

        processed_kwargs: dict[str, Any] = {}
        for name, field in event_class.model_fields.items():
            if name == id_field:
                continue

            if name in kwargs:
                # Natively coerce the given input using Pydantic's TypeAdapter for the field
                processed_kwargs[name] = TypeAdapter(field.annotation).validate_python(kwargs[name])
            elif field.default is not ... and field.default.__class__.__name__ != "PydanticUndefinedType":
                # Apply scalar defaults
                processed_kwargs[name] = field.default
            elif getattr(field, "default_factory", None) is not None:
                # Apply dynamic default factories
                if field.default_factory is not None:
                    processed_kwargs[name] = field.default_factory()  # type: ignore
            elif field.annotation is not None:  # pragma: no cover
                # If optional and missing, evaluate to None or leave omitted based on field definitions
                import contextlib
                from typing import Union, get_origin

                with contextlib.suppress(Exception):
                    import types

                    is_union = get_origin(field.annotation) in (Union, types.UnionType)
                    if is_union and type(None) in getattr(field.annotation, "__args__", []):
                        processed_kwargs[name] = None

        # Unvalidated fields not strictly in schema (kwargs passed through)
        for k, v in kwargs.items():
            if k not in processed_kwargs and k != id_field:
                processed_kwargs[k] = v

        # Construct the temporary instance devoid of an ID string strictly for hash compilation
        temp_event = event_class.model_construct(**processed_kwargs)

        # Calculate the deterministic hash from the securely coerced and defaulted payload
        event_hash = calculate_event_hash(temp_event.model_dump(exclude={id_field}, exclude_none=False))

        final_kwargs = dict(kwargs)

        # Inject the actual cryptographically sound ID to satisfy strict validators
        if id_field in event_class.model_fields:
            final_kwargs[id_field] = event_hash

        # Instantiate normally to execute all formal model_validators
        return event_class(**final_kwargs)
