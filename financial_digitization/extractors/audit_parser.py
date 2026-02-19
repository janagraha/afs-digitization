from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class EvidenceBlock:
    label: str
    text: str
    confidence: float


class AuditorReportParser:
    """Rule-based NLP parser for auditor reports with evidence blocks."""

    _opinion_pattern = re.compile(
        r"(unmodified opinion|qualified opinion|adverse opinion|disclaimer of opinion)",
        re.IGNORECASE,
    )

    def parse(self, report_text: str) -> dict[str, object]:
        paragraphs = [part.strip() for part in re.split(r"\n\s*\n", report_text) if part.strip()]
        evidence_blocks: list[EvidenceBlock] = []

        opinion = self._extract_opinion(report_text)
        key_matters = self._extract_key_audit_matters(paragraphs)
        basis = self._extract_basis(paragraphs)

        if opinion:
            evidence_blocks.append(EvidenceBlock("opinion", opinion, 0.95))
        if basis:
            evidence_blocks.append(EvidenceBlock("basis", basis, 0.9))
        for matter in key_matters:
            evidence_blocks.append(EvidenceBlock("key_audit_matter", matter, 0.88))

        return {
            "opinion": opinion or "UNKNOWN",
            "basis_for_opinion": basis,
            "key_audit_matters": key_matters,
            "emphasis_of_matter": self._extract_emphasis(paragraphs),
            "requires_manual_review": opinion is None,
            "review_reasons": ["AUDIT_OPINION_NOT_FOUND"] if opinion is None else [],
            "evidence_blocks": [block.__dict__ for block in evidence_blocks],
        }

    def _extract_opinion(self, text: str) -> str | None:
        match = self._opinion_pattern.search(text)
        return match.group(1).title() if match else None

    @staticmethod
    def _extract_basis(paragraphs: list[str]) -> str:
        for paragraph in paragraphs:
            if "basis for" in paragraph.lower() and "opinion" in paragraph.lower():
                return paragraph
        return ""

    @staticmethod
    def _extract_key_audit_matters(paragraphs: list[str]) -> list[str]:
        return [paragraph for paragraph in paragraphs if "key audit matter" in paragraph.lower()]

    @staticmethod
    def _extract_emphasis(paragraphs: list[str]) -> str:
        for paragraph in paragraphs:
            if "emphasis of matter" in paragraph.lower():
                return paragraph
        return ""
