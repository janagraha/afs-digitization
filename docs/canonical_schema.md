# Canonical Financial Statement Schema (v1.0.0)

This document defines the layout-independent target model for:
- Balance Sheet
- Balance Sheet Schedules
- Income & Expenditure
- Income & Expenditure Schedules
- Cash Flow
- Auditor Report

## Master envelope
The final output contract is represented by `schemas/1.0.0/master_envelope.schema.json` and requires:
- job metadata and source file hashes
- statement outputs and confidence maps
- validation report summary
- manual review flags and reasons
- evidence index for traceability

## Design principles
1. Preserve `raw` and parsed `value` independently for all numeric fields.
2. Every critical extraction should carry evidence (page, bbox, source).
3. Deterministic validators are authoritative.
4. Any failed high-severity validation must trigger manual review.
