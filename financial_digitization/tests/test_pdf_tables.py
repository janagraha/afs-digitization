from __future__ import annotations

from financial_digitization.extractors.pdf_tables import ExtractedTable, extract_tables_from_text
from web_digitizer import DigitizationHandler


def test_extract_tables_from_text_detects_multiple_tables() -> None:
    text = (
        "Particulars    FY2023    FY2022\n"
        "Cash and Bank    1000    900\n"
        "\n"
        "Liabilities    FY2023    FY2022\n"
        "Borrowings    400    350\n"
    )
    tables = extract_tables_from_text([text])

    assert len(tables) == 2
    assert tables[0].rows[0] == ["Particulars", "FY2023", "FY2022"]
    assert tables[1].rows[1] == ["Borrowings", "400", "350"]


def test_tables_to_rows_falls_back_to_page_text_when_no_tables() -> None:
    rows = DigitizationHandler._tables_to_rows([], ["Revenue 100\nExpense 75"])
    assert rows[0] == ["Account Code", "Particulars", "Current Year Amount", "Previous Year Amount"]
    assert rows[1][1:] == ["Revenue", "100", ""]
    assert rows[2][1:] == ["Expense", "75", ""]


def test_tables_to_rows_exports_extracted_table_grid() -> None:
    table = ExtractedTable(page=2, index=1, rows=[["Particulars", "FY2023-24", "FY2022-23"], ["Cash", "100", "80"]])
    rows = DigitizationHandler._tables_to_rows([table], [])
    assert rows[0] == [
        "Account Code",
        "Particulars",
        "Current Year Amount (FY2023-24)",
        "Previous Year Amount (FY2022-23)",
    ]
    assert rows[1][1:] == ["Cash", "100", "80"]


def test_split_particulars_and_amounts_detaches_embedded_current_year_amount() -> None:
    particulars, current_year, previous_year = DigitizationHandler._split_particulars_and_amounts(
        ["Less: Schedule I1(A) 4,366,215,311.00-", "3,574,623,087.00-"]
    )
    assert particulars == "Less: Schedule I1(A)"
    assert current_year == "4,366,215,311.00-"
    assert previous_year == "3,574,623,087.00-"
