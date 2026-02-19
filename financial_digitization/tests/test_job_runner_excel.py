from __future__ import annotations

from pathlib import Path
from zipfile import ZipFile

from financial_digitization.pipelines.job_runner import ETLJobRunner


def test_job_runner_creates_excel_per_pdf(tmp_path: Path) -> None:
    pdf1 = tmp_path / "balance_sheet.pdf"
    pdf2 = tmp_path / "audit_report.pdf"
    pdf1.write_bytes(b"pdf-one")
    pdf2.write_bytes(b"pdf-two")

    runner = ETLJobRunner(output_root=tmp_path / "out")
    envelope = runner.run([pdf1, pdf2], ["Balance Sheet assets", "Independent Auditor true and fair"])

    job_dir = tmp_path / "out" / str(envelope["job"]["job_id"])
    assert (job_dir / "balance_sheet.xlsx").exists()
    assert (job_dir / "audit_report.xlsx").exists()

    with ZipFile(job_dir / "balance_sheet.xlsx") as zf:
        names = set(zf.namelist())
        assert "[Content_Types].xml" in names
        assert "xl/workbook.xml" in names
        assert "xl/worksheets/sheet1.xml" in names
