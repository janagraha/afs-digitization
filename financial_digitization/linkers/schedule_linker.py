from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ScheduleReference:
    statement_line_item: str
    schedule_id: str
    evidence: str
    confidence: float


class ScheduleLinker:
    """Detect and link schedule references from extracted statement text."""

    _schedule_pattern = re.compile(r"(?:schedule|sch\.?|note)\s*([a-z0-9-]+)", re.IGNORECASE)

    def detect_references(self, line_items: list[str]) -> list[ScheduleReference]:
        refs: list[ScheduleReference] = []
        for item in line_items:
            match = self._schedule_pattern.search(item)
            if not match:
                continue
            schedule_id = match.group(1).upper()
            refs.append(
                ScheduleReference(
                    statement_line_item=item,
                    schedule_id=schedule_id,
                    evidence=match.group(0),
                    confidence=self._confidence(item, match.group(0)),
                )
            )
        return refs

    def build_index(self, schedule_pages: dict[str, list[str]]) -> dict[str, dict[str, object]]:
        return {
            schedule_id.upper(): {
                "page_refs": pages,
                "anchor_text": pages[0] if pages else "",
            }
            for schedule_id, pages in schedule_pages.items()
        }

    def link(self, line_items: list[str], schedule_pages: dict[str, list[str]]) -> dict[str, object]:
        refs = self.detect_references(line_items)
        index = self.build_index(schedule_pages)
        linked: list[dict[str, object]] = []
        unlinked: list[dict[str, object]] = []

        for ref in refs:
            if ref.schedule_id in index:
                linked.append(
                    {
                        "line_item": ref.statement_line_item,
                        "schedule_id": ref.schedule_id,
                        "evidence": ref.evidence,
                        "confidence": ref.confidence,
                        "target": index[ref.schedule_id],
                    }
                )
            else:
                unlinked.append(
                    {
                        "line_item": ref.statement_line_item,
                        "schedule_id": ref.schedule_id,
                        "evidence": ref.evidence,
                        "confidence": ref.confidence,
                        "reason": "SCHEDULE_NOT_FOUND",
                    }
                )

        return {
            "linked": linked,
            "unlinked": unlinked,
            "requires_manual_review": bool(unlinked),
            "review_reasons": [f"UNLINKED_SCHEDULE_{item['schedule_id']}" for item in unlinked],
        }

    @staticmethod
    def _confidence(item: str, evidence: str) -> float:
        base = 0.75
        if "note" in evidence.lower():
            base += 0.1
        if any(ch.isdigit() for ch in evidence):
            base += 0.1
        if len(item) < 12:
            base -= 0.1
        return round(min(base, 0.99), 2)
