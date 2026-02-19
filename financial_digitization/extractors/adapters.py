from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from financial_digitization.models.contracts import ExtractionBlock


@dataclass(frozen=True)
class ReconstructedTable:
    page: int
    headers: list[str]
    rows: list[list[str]]
    source: str


class PDFTextExtractor:
    """Robust extraction adapter with file fallback and table reconstruction."""

    def extract(self, page_texts: list[str], file_path: Path | None = None) -> list[ExtractionBlock]:
        resolved_texts = page_texts or self._fallback_file_extraction(file_path)
        return [
            ExtractionBlock(page=i + 1, text=text, bbox=[0, 0, 1, 1], source="pdf_text", confidence=0.99)
            for i, text in enumerate(resolved_texts)
        ]

    def reconstruct_tables(self, page_texts: list[str]) -> list[ReconstructedTable]:
        tables: list[ReconstructedTable] = []
        for page_number, text in enumerate(page_texts, start=1):
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            table_lines = [line for line in lines if self._looks_like_row(line)]
            if not table_lines:
                continue

            first_data_cols = len(self._split_row(table_lines[0]))
            for candidate in lines:
                candidate_cols = len(self._split_row(candidate))
                if candidate_cols == first_data_cols and candidate not in table_lines and not any(ch.isdigit() for ch in candidate):
                    table_lines.insert(0, candidate)
                    break

            if len(table_lines) < 2:
                continue

            split_rows = [self._split_row(line) for line in table_lines]
            max_cols = max(len(row) for row in split_rows)
            normalized = [row + [""] * (max_cols - len(row)) for row in split_rows]

            headers = normalized[0]
            body = normalized[1:]
            tables.append(
                ReconstructedTable(
                    page=page_number,
                    headers=headers,
                    rows=body,
                    source="pdf_text_table_heuristic",
                )
            )
        return tables

    def _fallback_file_extraction(self, file_path: Path | None) -> list[str]:
        if file_path is None or not file_path.exists():
            return []
        bytes_payload = file_path.read_bytes()[:16_000]
        try:
            decoded = bytes_payload.decode("utf-8", errors="ignore")
        except Exception:
            decoded = ""
        cleaned = re.sub(r"\s+", " ", decoded).strip()
        return [cleaned] if cleaned else []

    @staticmethod
    def _looks_like_row(line: str) -> bool:
        delimiter_hits = line.count("|") + len(re.findall(r"\s{2,}", line))
        digit_hits = len(re.findall(r"\d", line))
        return delimiter_hits > 0 and digit_hits > 0

    @staticmethod
    def _split_row(line: str) -> list[str]:
        if "|" in line:
            return [cell.strip() for cell in line.split("|") if cell.strip()]
        return [cell.strip() for cell in re.split(r"\s{2,}", line) if cell.strip()]


class OCRExtractor:
    """OCR adapter with confidence degradation for noisy text."""

    def extract(self, ocr_texts: list[str]) -> list[ExtractionBlock]:
        blocks: list[ExtractionBlock] = []
        for i, text in enumerate(ocr_texts):
            confidence = 0.85
            if self._looks_noisy(text):
                confidence = 0.7
            blocks.append(
                ExtractionBlock(page=i + 1, text=text, bbox=[0, 0, 1, 1], source="ocr", confidence=confidence)
            )
        return blocks

    @staticmethod
    def _looks_noisy(text: str) -> bool:
        if not text:
            return True
        alnum = sum(char.isalnum() for char in text)
        return (alnum / max(len(text), 1)) < 0.5
