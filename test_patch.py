from coreason_manifest.spec.ontology import ObservationEvent, DynamicRoutingManifest
from pydantic.fields import FieldInfo

ObservationEvent.model_fields["embedded_routing_manifest"] = FieldInfo(
    annotation=DynamicRoutingManifest | None,
    default=None,
    description="An explicit, discriminated field for conditional routing."
)
ObservationEvent.model_rebuild(force=True)

print(ObservationEvent.model_fields.keys())
print("embedded_routing_manifest" in ObservationEvent.model_fields)
