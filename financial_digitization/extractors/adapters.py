from __future__ import annotations

from financial_digitization.models.contracts import ExtractionBlock


class PDFTextExtractor:
    def extract(self, page_texts: list[str]) -> list[ExtractionBlock]:
        return [
            ExtractionBlock(page=i + 1, text=text, bbox=[0, 0, 1, 1], source="pdf_text", confidence=0.99)
            for i, text in enumerate(page_texts)
        ]


class OCRExtractor:
    def extract(self, ocr_texts: list[str]) -> list[ExtractionBlock]:
        return [
            ExtractionBlock(page=i + 1, text=text, bbox=[0, 0, 1, 1], source="ocr", confidence=0.85)
            for i, text in enumerate(ocr_texts)
        ]
