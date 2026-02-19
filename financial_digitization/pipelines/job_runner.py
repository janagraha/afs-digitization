from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from financial_digitization.exporters.excel_writer import write_excel
from financial_digitization.extractors.adapters import OCRExtractor, PDFTextExtractor
from financial_digitization.extractors.audit_parser import AuditorReportParser
from financial_digitization.linkers.schedule_linker import ScheduleLinker
from financial_digitization.mappers.semantic_mapper import SemanticMapper
from financial_digitization.normalizers.numeric import parse_amount
from financial_digitization.pipelines.classifier import classify_document
from financial_digitization.pipelines.job_store import JobRecord, PersistentJobStore
from financial_digitization.validators.financial_rules import FinancialValidator, summarize_findings


class ETLJobRunner:
    def __init__(self, output_root: Path, max_retries: int = 2) -> None:
        self.output_root = output_root
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.max_retries = max_retries
        self.job_store = PersistentJobStore(self.output_root / "job_store")
        self.pdf_extractor = PDFTextExtractor()
        self.ocr_extractor = OCRExtractor()
        self.schedule_linker = ScheduleLinker()
        self.audit_parser = AuditorReportParser()

    def _hash_file(self, file_path: Path) -> str:
        return hashlib.sha256(file_path.read_bytes()).hexdigest()

    def _job_dir(self, job_id: str) -> Path:
        path = self.output_root / job_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def run(self, file_paths: list[Path], page_texts: list[str]) -> dict[str, object]:
        job_id = str(uuid4())
        self.job_store.bump_metric("submitted")
        record = JobRecord(job_id=job_id, status="submitted", attempts=0, payload={"files": [p.name for p in file_paths]})
        self.job_store.upsert(record)

        last_error = ""
        for attempt in range(1, self.max_retries + 2):
            try:
                envelope = self._run_once(job_id=job_id, file_paths=file_paths, page_texts=page_texts)
                self.job_store.upsert(JobRecord(job_id=job_id, status="completed", attempts=attempt, payload=envelope))
                self.job_store.bump_metric("succeeded")
                return envelope
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                self.job_store.upsert(
                    JobRecord(job_id=job_id, status="retrying", attempts=attempt, payload=record.payload, error=last_error)
                )
                if attempt <= self.max_retries:
                    self.job_store.bump_metric("retried")
                    continue
                failed_record = JobRecord(
                    job_id=job_id,
                    status="failed",
                    attempts=attempt,
                    payload=record.payload,
                    error=last_error,
                )
                self.job_store.upsert(failed_record)
                self.job_store.bump_metric("failed")
                self.job_store.move_to_dlq(failed_record)
                raise
        raise RuntimeError(f"Job failed unexpectedly: {last_error}")

    def _run_once(self, job_id: str, file_paths: list[Path], page_texts: list[str]) -> dict[str, object]:
        job_dir = self._job_dir(job_id)
        if any("FORCE_ERROR" in text for text in page_texts):
            raise RuntimeError("Forced processing error for retry/DLQ validation")

        source_files = [
            {
                "filename": p.name,
                "size_bytes": p.stat().st_size,
                "sha256": self._hash_file(p),
                "page_count": len(page_texts),
            }
            for p in file_paths
        ]

        pdf_blocks = self.pdf_extractor.extract(page_texts, file_paths[0] if file_paths else None)
        ocr_blocks = self.ocr_extractor.extract(page_texts)
        tables = self.pdf_extractor.reconstruct_tables([block.text for block in pdf_blocks])
        classification = classify_document([block.text for block in pdf_blocks])

        parsed_demo_amount = parse_amount("1,23,45,000")
        mapper = SemanticMapper({"Plant & Machinery": "balance_sheet.assets.non_current_assets.plant_machinery"})
        mapping_demo = mapper.resolve("Plant and Machinery")

        schedule_linking = self.schedule_linker.link(
            line_items=[block.text for block in pdf_blocks],
            schedule_pages={"I": ["Schedule I - Fixed Assets"], "2": ["Schedule 2 - Loans"]},
        )

        audit_text = "\n\n".join(block.text for block in pdf_blocks)
        audit_report = self.audit_parser.parse(audit_text)

        validator = FinancialValidator(tolerance_absolute=1)
        findings = [
            validator.check_balance_sheet(100_000, 100_000),
            validator.check_cash_flow(5000, -500, 4500),
            validator.check_income_expenditure(12000, 1000, 11000, 2000),
        ]
        validation = summarize_findings(findings)

        envelope = {
            "schema_version": "1.0.0",
            "job": {
                "job_id": job_id,
                "source_files": source_files,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "processed_at": datetime.now(timezone.utc).isoformat(),
            },
            "entity": {"ulb_name": "", "ulb_code": "", "state": ""},
            "statement_periods": ["FY2023-24", "FY2022-23"],
            "source_units": {"currency": "INR", "reported_unit": "INR"},
            "outputs": {
                "balance_sheet": {},
                "income_expenditure": {},
                "cash_flow": {},
                "audit_report": audit_report,
            },
            "confidence": {
                "overall": 0.82,
                "by_statement": {
                    "classification": 1.0 - (0.1 if classification["requires_manual_review"] else 0),
                    "ocr_quality": round(sum(b.confidence for b in ocr_blocks) / max(len(ocr_blocks), 1), 2),
                    "schedule_linking": 1.0 - (0.2 if schedule_linking["requires_manual_review"] else 0),
                },
            },
            "validation": validation,
            "requires_manual_review": any(
                [
                    classification["requires_manual_review"],
                    validation["requires_manual_review"],
                    schedule_linking["requires_manual_review"],
                    audit_report["requires_manual_review"],
                ]
            ),
            "review_reasons": (
                classification["review_reasons"]
                + validation["review_reasons"]
                + schedule_linking["review_reasons"]
                + audit_report["review_reasons"]
            ),
            "evidence_index": {
                "demo_parsed_amount": parsed_demo_amount.__dict__,
                "demo_mapping": mapping_demo,
                "page_map": classification["page_map"],
                "tables": [table.__dict__ for table in tables],
                "schedule_links": schedule_linking,
                "audit_evidence": audit_report["evidence_blocks"],
            },
        }

        (job_dir / "mapped_canonical.json").write_text(json.dumps(envelope, indent=2), encoding="utf-8")
        (job_dir / "validation_report.json").write_text(json.dumps(validation, indent=2), encoding="utf-8")
        (job_dir / "job_log.jsonl").write_text(
            json.dumps({"event": "job_completed", "job_id": job_id, "timestamp": datetime.now(timezone.utc).isoformat()})
            + "\n",
            encoding="utf-8",
        )

        for statement in ("balance_sheet", "audit_report"):
            write_excel(
                job_dir / f"{statement}.xlsx",
                rows=[
                    ["job_id", job_id],
                    ["statement", statement],
                    ["requires_manual_review", envelope["requires_manual_review"]],
                    ["review_reasons", ", ".join(envelope["review_reasons"])],
                ],
            )
        return envelope


def _collect_pdfs(input_path: Path) -> list[Path]:
    if input_path.is_file() and input_path.suffix.lower() == ".pdf":
        return [input_path]
    if input_path.is_dir():
        return sorted(input_path.glob("*.pdf"))
    return []


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Financial digitization ETL (skeleton runner).")
    parser.add_argument("--input", required=True, help="PDF file or folder containing PDFs")
    parser.add_argument("--out", required=True, help="Output root folder for job artifacts")
    args = parser.parse_args(argv)

    input_path = Path(args.input)
    output_root = Path(args.out)

    pdfs = _collect_pdfs(input_path)
    if not pdfs:
        raise SystemExit(f"No PDFs found at: {input_path}")

    runner = ETLJobRunner(output_root=output_root)

    for pdf in pdfs:
        envelope = runner.run(file_paths=[pdf], page_texts=[""])
        current_job_id = envelope["job"]["job_id"]
        print(f"OK: {pdf.name} -> {output_root / current_job_id}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
