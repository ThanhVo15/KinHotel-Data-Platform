from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

@dataclass
class LoadArtifact:
    kind: str          # "parquet" / "sheet"
    id: str
    name: str
    link: str
    rows: int
    meta: Dict[str, Any]

@dataclass
class LoadSummary:
    dataset: str
    branch_id: Optional[int]
    artifacts: List[LoadArtifact]

class AbstractLoader(ABC):
    @abstractmethod
    def load(self, *, dataset: str, branch_id: Optional[int], records: Iterable[Dict[str, Any]], **kwargs) -> LoadSummary:
        ...
