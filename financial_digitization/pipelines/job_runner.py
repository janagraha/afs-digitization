from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from financial_digitization.mappers.semantic_mapper import SemanticMapper
from financial_digitization.normalizers.numeric import parse_amount
from financial_digitization.pipelines.classifier import classify_document
from financial_digitization.validators.financial_rules import FinancialValidator, summarize_findings


class ETLJobRunner:
    def __init__(self, output_root: Path) -> None:
        self.output_root = output_root
        self.output_root.mkdir(parents=True, exist_ok=True)

    def _hash_file(self, file_path: Path) -> str:
        return hashlib.sha256(file_path.read_bytes()).hexdigest()

    def _job_dir(self, job_id: str) -> Path:
        path = self.output_root / job_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def run(self, file_paths: list[Path], page_texts: list[str]) -> dict[str, object]:
        job_id = str(uuid4())
        job_dir = self._job_dir(job_id)

        source_files = [
            {
                "filename": p.name,
                "size_bytes": p.stat().st_size,
                "sha256": self._hash_file(p),
                "page_count": len(page_texts),
            }
            for p in file_paths
        ]

        classification = classify_document(page_texts)

        parsed_demo_amount = parse_amount("1,23,45,000")
        mapper = SemanticMapper({"Plant & Machinery": "balance_sheet.assets.non_current_assets.plant_machinery"})
        mapping_demo = mapper.resolve("Plant and Machinery")

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
                "audit_report": {},
            },
            "confidence": {"overall": 0.8, "by_statement": {}},
            "validation": validation,
            "requires_manual_review": classification["requires_manual_review"] or validation["requires_manual_review"],
            "review_reasons": classification["review_reasons"] + validation["review_reasons"],
            "evidence_index": {
                "demo_parsed_amount": parsed_demo_amount.__dict__,
                "demo_mapping": mapping_demo,
                "page_map": classification["page_map"],
            },
        }

        (job_dir / "mapped_canonical.json").write_text(json.dumps(envelope, indent=2), encoding="utf-8")
        (job_dir / "validation_report.json").write_text(json.dumps(validation, indent=2), encoding="utf-8")
        (job_dir / "job_log.jsonl").write_text(
            json.dumps({"event": "job_completed", "job_id": job_id, "timestamp": datetime.now(timezone.utc).isoformat()}) + "\n",
            encoding="utf-8",
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
        job_id = envelope["job"]["job_id"]
        print(f"OK: {pdf.name} -> {output_root / job_id}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
