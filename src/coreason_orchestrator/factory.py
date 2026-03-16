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

        # If the class has the ID field, inject a valid temporary one to satisfy Pydantic
        # strict regexes (e.g., W3C DIDs, SHA-256 hashes) without breaking model_config
        # and serializers, so that we can instantiate the model natively to enforce defaults.
        temp_kwargs = dict(kwargs)
        if id_field in event_class.model_fields:
            # We inject a 64-character hash pattern that satisfies standard cryptographic CID constraints
            temp_kwargs[id_field] = "0" * 64

        # Instantiate the native model to ensure Pydantic applies defaults, coercion, and custom validation
        temp_event = event_class(**temp_kwargs)

        # Calculate the deterministic hash from the fully validated output (excluding the ID field)
        event_hash = calculate_event_hash(temp_event.model_dump(exclude={id_field}))

        final_kwargs = dict(kwargs)

        # Inject the true ID only if the class actually supports it
        if id_field in event_class.model_fields:
            final_kwargs[id_field] = event_hash

        return event_class(**final_kwargs)
