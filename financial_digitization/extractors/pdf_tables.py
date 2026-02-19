from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

try:
    from pypdf import PdfReader  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency at runtime
    PdfReader = None

_COLUMN_SPLIT_RE = re.compile(r"\t+|\s{2,}")


@dataclass(frozen=True)
class ExtractedTable:
    page: int
    index: int
    rows: list[list[str]]


def extract_page_texts(pdf_path: Path) -> list[str]:
    if PdfReader is None:
        return []
    try:
        reader = PdfReader(str(pdf_path))
    except Exception:  # pragma: no cover - parser/runtime fallback
        return []

    extracted_texts: list[str] = []
    for page in reader.pages:
        try:
            extracted_texts.append((page.extract_text() or "").strip())
        except Exception:  # pragma: no cover - page-level extraction fallback
            extracted_texts.append("")
    return extracted_texts


def extract_tables(pdf_path: Path, page_texts: list[str] | None = None) -> list[ExtractedTable]:
    tables = _extract_tables_with_pdfplumber(pdf_path)
    if tables:
        return tables
    return extract_tables_from_text(page_texts if page_texts is not None else extract_page_texts(pdf_path))


def extract_tables_from_text(page_texts: list[str]) -> list[ExtractedTable]:
    tables: list[ExtractedTable] = []

    for page_index, text in enumerate(page_texts, start=1):
        next_table_index = 1
        current_rows: list[list[str]] = []

        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                if current_rows:
                    tables.append(ExtractedTable(page=page_index, index=next_table_index, rows=_pad_rows(current_rows)))
                    next_table_index += 1
                    current_rows = []
                continue

            columns = [chunk.strip() for chunk in _COLUMN_SPLIT_RE.split(line) if chunk.strip()]
            if len(columns) >= 2:
                current_rows.append(columns)
                continue

            if current_rows:
                tables.append(ExtractedTable(page=page_index, index=next_table_index, rows=_pad_rows(current_rows)))
                next_table_index += 1
                current_rows = []

        if current_rows:
            tables.append(ExtractedTable(page=page_index, index=next_table_index, rows=_pad_rows(current_rows)))

    return tables


def _extract_tables_with_pdfplumber(pdf_path: Path) -> list[ExtractedTable]:
    try:
        import pdfplumber  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover - optional dependency at runtime
        return []

    extracted: list[ExtractedTable] = []
    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page_index, page in enumerate(pdf.pages, start=1):
                page_tables = page.extract_tables() or []
                for table_index, table in enumerate(page_tables, start=1):
                    normalized = _normalize_table(table)
                    if normalized:
                        extracted.append(ExtractedTable(page=page_index, index=table_index, rows=normalized))
    except Exception:  # pragma: no cover - parser/runtime fallback
        return []
    return extracted


def _normalize_table(table: list[list[str | None]] | None) -> list[list[str]]:
    if not table:
        return []

    cleaned_rows: list[list[str]] = []
    for row in table:
        if row is None:
            continue
        normalized_row = ["" if cell is None else str(cell).replace("\n", " ").strip() for cell in row]
        if any(normalized_row):
            cleaned_rows.append(normalized_row)

    if not cleaned_rows:
        return []
    return _pad_rows(cleaned_rows)


def _pad_rows(rows: list[list[str]]) -> list[list[str]]:
    width = max(len(row) for row in rows)
    return [row + [""] * (width - len(row)) for row in rows]
