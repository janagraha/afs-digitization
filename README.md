# Financial Statement Digitization ETL (Deterministic + Auditable)

This repository provides a production-oriented scaffold for digitizing financial statements from PDF/paper sources into a canonical, validated JSON output.

## Implemented modules
- `schemas/1.0.0/`: JSON Schemas for master envelope, balance sheet, income & expenditure, cash flow, audit report.
- `validators/schema_validator.py`: JSON Schema validator wrapper.
- `pipelines/classifier.py`: deterministic page classification with confidence + review triggers.
- `extractors/adapters.py`: extraction contracts for PDF text and OCR outputs.
- `normalizers/numeric.py`: Indian-format numeric and period normalization.
- `mappers/semantic_mapper.py`: dictionary → normalized → fuzzy semantic mapping.
- `validators/financial_rules.py`: deterministic financial equation checks + summarized report.
- `pipelines/job_runner.py`: ETL orchestration skeleton persisting mapped output, validation report, log, and one Excel workbook (`.xlsx`) per source PDF.

## Phase-by-phase implementation status
1. **Canonical schema** ✅
2. **JSON Schema validators + tests** ✅
3. **Ingestion/job control skeleton** ✅
4. **Classification** ✅
5. **Extraction contracts** ✅
6. **Normalization** ✅
7. **Semantic mapping** ✅
8. **Schedule linking** ➖ (planned extension)
9. **Financial validation** ✅
10. **Auditor report parser** ➖ (schema ready, parser extension pending)
11. **Excel export per PDF** ✅
12. **Confidence + HITL hooks** ✅
13. **Ops hardening (DLQ/retries/metrics)** ➖ (next iteration)
14. **Security/governance controls** ➖ (next iteration)

## Run tests
```bash
python -m pip install -e .[dev]
pytest
```

## Next production steps
- Add robust PDF/OCR engine integrations and table reconstruction.
- Add schedule reference detection and linking engine.
- Add auditor report NLP parser with evidence blocks.
- Add persistent job store, retries, DLQ, and metrics.
