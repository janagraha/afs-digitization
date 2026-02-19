from __future__ import annotations

import json
from pathlib import Path

import pytest

from financial_digitization.extractors.adapters import PDFTextExtractor
from financial_digitization.extractors.audit_parser import AuditorReportParser
from financial_digitization.linkers.schedule_linker import ScheduleLinker
from financial_digitization.pipelines.job_runner import ETLJobRunner


def test_table_reconstruction_from_text() -> None:
    extractor = PDFTextExtractor()
    tables = extractor.reconstruct_tables(
        [
            "Line\nParticulars  Amount\nTaxes  1200\nFees  300",
        ]
    )
    assert len(tables) == 1
    assert tables[0].headers == ["Particulars", "Amount"]
    assert tables[0].rows[0] == ["Taxes", "1200"]


def test_schedule_linking_detects_unlinked() -> None:
    linker = ScheduleLinker()
    result = linker.link(
        ["Fixed assets as per Schedule 1", "Loan liabilities as per Note 7"],
        schedule_pages={"1": ["Schedule 1 - Fixed Assets"]},
    )
    assert len(result["linked"]) == 1
    assert len(result["unlinked"]) == 1
    assert result["requires_manual_review"] is True


def test_auditor_parser_extracts_evidence() -> None:
    parser = AuditorReportParser()
    parsed = parser.parse(
        "Unmodified opinion\n\n"
        "Basis for Opinion We conducted our audit in accordance with standards.\n\n"
        "Key Audit Matter: Revenue recognition near year-end."
    )
    assert parsed["opinion"] == "Unmodified Opinion"
    assert parsed["basis_for_opinion"]
    assert parsed["key_audit_matters"]
    assert parsed["evidence_blocks"]


def test_job_store_retry_and_dlq(tmp_path: Path) -> None:
    pdf = tmp_path / "input.pdf"
    pdf.write_bytes(b"content")
    runner = ETLJobRunner(output_root=tmp_path / "out", max_retries=1)

    with pytest.raises(RuntimeError):
        runner.run([pdf], ["FORCE_ERROR"])

    metrics = json.loads((tmp_path / "out" / "job_store" / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["submitted"] == 1
    assert metrics["retried"] == 1
    assert metrics["failed"] == 1
    assert metrics["dlq"] == 1
    assert any((tmp_path / "out" / "job_store" / "dlq").iterdir())
