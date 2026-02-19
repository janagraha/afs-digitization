from __future__ import annotations

from financial_digitization.models.contracts import ValidationFinding


class FinancialValidator:
    def __init__(self, tolerance_absolute: float = 1.0) -> None:
        self.tolerance_absolute = tolerance_absolute

    def _make_finding(self, rule: str, expected: float, actual: float, severity: str = "HIGH") -> ValidationFinding:
        variance = actual - expected
        status = "PASSED" if abs(variance) <= self.tolerance_absolute else "FAILED"
        return ValidationFinding(
            validation_status=status,
            rule=rule,
            expected=expected,
            actual=actual,
            variance=variance,
            tolerance=self.tolerance_absolute,
            severity=severity,
            message=f"{rule} {'ok' if status == 'PASSED' else 'mismatch'}",
        )

    def check_balance_sheet(self, total_assets: float, total_equity_liabilities: float) -> ValidationFinding:
        return self._make_finding("BS_BALANCE", total_assets, total_equity_liabilities)

    def check_cash_flow(self, opening: float, net_change: float, closing: float) -> ValidationFinding:
        return self._make_finding("CF_BALANCE", opening + net_change, closing)

    def check_income_expenditure(self, revenue: float, other_income: float, expenditure: float, surplus: float) -> ValidationFinding:
        return self._make_finding("IE_SURPLUS", (revenue + other_income) - expenditure, surplus)

    def check_crossfoot(self, parent_total: float, child_sum: float, rule: str = "CROSSFOOT") -> ValidationFinding:
        return self._make_finding(rule, parent_total, child_sum, severity="MEDIUM")


def summarize_findings(findings: list[ValidationFinding]) -> dict[str, object]:
    failures = [f for f in findings if f.validation_status == "FAILED"]
    return {
        "validation_status": "FAILED" if failures else "PASSED",
        "findings": [f.__dict__ for f in findings],
        "requires_manual_review": bool(failures),
        "review_reasons": [f.rule for f in failures],
    }
