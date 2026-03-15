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
        # If the class has an event_id field, inject a temporary one to satisfy Pydantic
        # so that we can instantiate a temporary model.
        temp_kwargs = dict(kwargs)
        if "event_id" in event_class.model_fields:
            temp_kwargs["event_id"] = ""

        # Instantiate a temporary model to ensure Pydantic applies defaults and type coercion
        temp_event = event_class(**temp_kwargs)

        # Calculate the deterministic hash from the fully validated output (excluding event_id)
        event_hash = calculate_event_hash(temp_event.model_dump(exclude={"event_id"}))

        final_kwargs = dict(kwargs)

        # Inject the true event_id only if the class actually supports it
        if "event_id" in event_class.model_fields:
            final_kwargs["event_id"] = event_hash

        return event_class(**final_kwargs)
