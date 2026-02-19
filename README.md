# Financial Statement Digitization ETL (Deterministic + Auditable)

This repository provides a production-oriented scaffold for digitizing financial statements from PDF/paper sources into a canonical, validated JSON output.

## Implemented modules
- `schemas/1.0.0/`: JSON Schemas for master envelope, balance sheet, income & expenditure, cash flow, audit report.
- `validators/schema_validator.py`: JSON Schema validator wrapper.
- `pipelines/classifier.py`: deterministic page classification with confidence + review triggers.
- `extractors/adapters.py`: robust PDF/OCR adapters with extraction fallback and heuristic table reconstruction.
- `normalizers/numeric.py`: Indian-format numeric and period normalization.
- `mappers/semantic_mapper.py`: dictionary → normalized → fuzzy semantic mapping.
- `validators/financial_rules.py`: deterministic financial equation checks + summarized report.
- `pipelines/job_runner.py`: ETL orchestration skeleton persisting mapped output, validation report, and log.

## Phase-by-phase implementation status
1. **Canonical schema** ✅
2. **JSON Schema validators + tests** ✅
3. **Ingestion/job control skeleton** ✅
4. **Classification** ✅
5. **Extraction contracts** ✅
6. **Normalization** ✅
7. **Semantic mapping** ✅
8. **Schedule linking** ✅
9. **Financial validation** ✅
10. **Auditor report parser** ✅
11. **Confidence + HITL hooks** ✅
12. **Ops hardening (DLQ/retries/metrics)** ✅
13. **Security/governance controls** ➖ (next iteration)

## Run tests
```bash
python -m pip install -e .[dev]
pytest
```

## Next production steps
- Add production PDF/OCR backends (e.g., cloud OCR) behind the new adapter contracts.
- Expand schedule linker from regex matching to schema-aware cross-statement graph linking.
- Add advanced auditor parser models for nuanced legal qualifiers and multilingual reports.
- Export metrics to observability systems (Prometheus/OpenTelemetry) and add alerting.
