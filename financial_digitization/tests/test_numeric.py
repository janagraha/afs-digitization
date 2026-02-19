from financial_digitization.normalizers.numeric import normalize_period, parse_amount


def test_parse_amount_indian_format() -> None:
    parsed = parse_amount("1,23,45,000")
    assert parsed.value == 12345000.0
    assert parsed.parse_status == "parsed"


def test_parse_amount_parentheses_negative() -> None:
    parsed = parse_amount("(1,234)")
    assert parsed.value == -1234.0


def test_parse_amount_footnote_marker() -> None:
    parsed = parse_amount("123*")
    assert parsed.value == 123.0
    assert "FOOTNOTE_MARKER_REMOVED" in parsed.parse_warnings


def test_normalize_period() -> None:
    assert normalize_period("For the year ended 31 March 2024") == "FY2023-24"
