"""ELT Data Contracts Platform.

Batch ELT with declarative data contracts: extract -> contract gate
(fail-closed, with quarantine) -> load to a local warehouse -> DAG
transforms -> post-transform contracts and data tests -> run manifest.
"""

from .contracts import Contract, ColumnContract, load_contracts, validate_rows
from .extractors import Extractor, SourceSpec
from .loader import Warehouse
from .manifest import ManifestStore, RunManifest
from .orchestrator import Pipeline, ReferentialCheck
from .quality import DataQualityReport
from .transforms import Transform, TransformDAG

__version__ = "0.1.0"

__all__ = [
    "Contract",
    "ColumnContract",
    "load_contracts",
    "validate_rows",
    "Extractor",
    "SourceSpec",
    "Warehouse",
    "ManifestStore",
    "RunManifest",
    "Pipeline",
    "ReferentialCheck",
    "DataQualityReport",
    "Transform",
    "TransformDAG",
]
