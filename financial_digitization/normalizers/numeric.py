from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ParsedAmount:
    raw: str
    value: float | None
    parse_status: str
    parse_warnings: list[str]


FOOTNOTE_RE = re.compile(r"[*#]+$")


def parse_amount(raw: str) -> ParsedAmount:
    text = raw.strip()
    if text == "" or text == "-":
        return ParsedAmount(raw=raw, value=None, parse_status="blank", parse_warnings=[])

    warnings: list[str] = []
    cleaned = FOOTNOTE_RE.sub("", text)
    if cleaned != text:
        warnings.append("FOOTNOTE_MARKER_REMOVED")

    negative = cleaned.startswith("(") and cleaned.endswith(")")
    cleaned = cleaned.strip("()")
    cleaned = cleaned.replace("₹", "").replace(",", "").replace("–", "-").strip()

    try:
        value = float(cleaned)
    except ValueError:
        return ParsedAmount(raw=raw, value=None, parse_status="invalid", parse_warnings=warnings + ["UNPARSABLE"])

    if negative:
        value = -value
    return ParsedAmount(raw=raw, value=value, parse_status="parsed", parse_warnings=warnings)


def normalize_period(header: str) -> str | None:
    h = header.strip().lower()
    match = re.search(r"(20\d{2})\s*[-/]\s*(\d{2,4})", h)
    if match:
        y1, y2 = match.group(1), match.group(2)
        if len(y2) == 4:
            y2 = y2[-2:]
        return f"FY{y1}-{y2}"

    match = re.search(r"31\s*march\s*(20\d{2})", h)
    if match:
        year = int(match.group(1))
        return f"FY{year-1}-{str(year)[-2:]}"
    return None
