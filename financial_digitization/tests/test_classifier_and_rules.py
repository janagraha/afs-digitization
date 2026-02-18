from financial_digitization.pipelines.classifier import classify_document
from financial_digitization.validators.financial_rules import FinancialValidator, summarize_findings


def test_classifier_labels_pages() -> None:
    out = classify_document([
        "Balance Sheet equity and liabilities assets",
        "Independent Auditor report true and fair",
    ])
    assert out["page_map"][0]["section"] == "BALANCE_SHEET"
    assert out["page_map"][1]["section"] == "AUDIT_REPORT"


def test_validation_summary_fails_when_rule_fails() -> None:
    validator = FinancialValidator(tolerance_absolute=1)
    findings = [validator.check_balance_sheet(100, 98)]
    summary = summarize_findings(findings)
    assert summary["validation_status"] == "FAILED"
    assert summary["requires_manual_review"] is True
