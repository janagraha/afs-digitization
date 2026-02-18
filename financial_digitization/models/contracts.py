from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class PageClassification:
    page: int
    section: str
    confidence: float
    signals: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ExtractionBlock:
    page: int
    text: str
    bbox: list[float]
    source: str
    confidence: float


@dataclass(frozen=True)
class ValidationFinding:
    validation_status: str
    rule: str
    expected: float | None
    actual: float | None
    variance: float | None
    tolerance: float
    severity: str
    message: str


JSONDict = dict[str, Any]
