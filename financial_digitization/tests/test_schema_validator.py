from financial_digitization.validators.schema_validator import SchemaValidator


def _valid_amount(value: float = 1.0) -> dict:
    return {
        "raw": str(value),
        "value": value,
        "confidence": 1.0,
        "parse_status": "parsed",
        "parse_warnings": [],
    }


def test_master_envelope_schema_valid() -> None:
    payload = {
        "schema_version": "1.0.0",
        "job": {
            "job_id": "j1",
            "source_files": [{"filename": "a.pdf", "size_bytes": 10, "sha256": "x", "page_count": 1}],
            "created_at": "2024-01-01T00:00:00Z",
            "processed_at": "2024-01-01T00:00:01Z",
        },
        "entity": {"ulb_name": "U", "ulb_code": "001", "state": "KA"},
        "statement_periods": ["FY2023-24"],
        "source_units": {"currency": "INR", "reported_unit": "INR"},
        "outputs": {"balance_sheet": {}, "income_expenditure": {}, "cash_flow": {}, "audit_report": {}},
        "confidence": {"overall": 0.8, "by_statement": {}},
        "validation": {},
        "requires_manual_review": False,
        "review_reasons": [],
        "evidence_index": {},
    }
    assert SchemaValidator().validate(payload, "master_envelope.schema.json") == []


def test_balance_sheet_schema_rejects_missing_totals() -> None:
    payload = {
        "periods": {
            "FY2023-24": {
                "assets": {"non_current_assets": []},
                "equity_and_liabilities": {"equity": []}
            }
        }
    }
    errors = SchemaValidator().validate(payload, "balance_sheet.schema.json")
    assert errors
    assert any("totals" in e for e in errors)


def test_balance_sheet_schema_rejects_unknown_fields() -> None:
    payload = {
        "periods": {
            "FY2023-24": {
                "assets": {"non_current_assets": []},
                "equity_and_liabilities": {"equity": []},
                "totals": {
                    "total_assets": _valid_amount(1),
                    "total_equity_liabilities": _valid_amount(1)
                },
                "unexpected": "x",
            }
        }
    }
    errors = SchemaValidator().validate(payload, "balance_sheet.schema.json")
    assert errors
    assert any("Additional properties" in e for e in errors)
