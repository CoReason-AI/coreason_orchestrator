# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator

"""
The central nervous system, epistemic router, and ledger guardian
"""

from coreason_manifest.spec.ontology import DynamicRoutingManifest, ObservationEvent
from pydantic.fields import FieldInfo

__version__ = "0.1.0"
__author__ = "Akshaya M"
__email__ = "akshaya.movvar@coreason.ai"

__all__: list[str] = []

ObservationEvent.model_fields["embedded_routing_manifest"] = FieldInfo(
    annotation=DynamicRoutingManifest | None,  # type: ignore[arg-type]
    default=None,
)
ObservationEvent.model_rebuild(force=True)
