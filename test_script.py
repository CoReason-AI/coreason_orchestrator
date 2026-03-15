import inspect
import pprint

from coreason_manifest.spec.ontology import DynamicRoutingManifest

pprint.pprint(inspect.signature(DynamicRoutingManifest).parameters)
