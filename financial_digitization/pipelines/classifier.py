from __future__ import annotations

from financial_digitization.models.contracts import PageClassification

SECTION_RULES: dict[str, list[str]] = {
    "BALANCE_SHEET": ["balance sheet", "equity and liabilities", "assets"],
    "BALANCE_SHEET_SCHEDULE": ["schedule", "notes to balance sheet"],
    "INCOME_EXPENDITURE": ["income and expenditure", "surplus", "deficit"],
    "INCOME_EXPENDITURE_SCHEDULE": ["notes to income", "schedule"],
    "CASH_FLOW": ["cash flow", "net increase in cash"],
    "AUDIT_REPORT": ["independent auditor", "true and fair", "qualified opinion"],
}



def classify_page_text(page: int, text: str) -> PageClassification:
    t = text.lower()
    scores: dict[str, int] = {}
    hits: dict[str, list[str]] = {}

    for section, keywords in SECTION_RULES.items():
        matched = [kw for kw in keywords if kw in t]
        if matched:
            scores[section] = len(matched)
            hits[section] = matched

    if not scores:
        return PageClassification(page=page, section="OTHER/ANNEXURE", confidence=0.5, signals=[])

    best = max(scores, key=scores.get)
    total = sum(scores.values())
    confidence = round(scores[best] / total, 2)
    return PageClassification(page=page, section=best, confidence=confidence, signals=hits.get(best, []))



def classify_document(page_texts: list[str], threshold: float = 0.75) -> dict[str, object]:
    page_map = [classify_page_text(i + 1, text) for i, text in enumerate(page_texts)]
    requires_review = any(item.confidence < threshold for item in page_map)
    return {
        "page_map": [item.__dict__ for item in page_map],
        "requires_manual_review": requires_review,
        "review_reasons": [
            f"LOW_CLASSIFICATION_CONFIDENCE_PAGE_{item.page}"
            for item in page_map
            if item.confidence < threshold
        ],
    }
