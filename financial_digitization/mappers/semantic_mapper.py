from __future__ import annotations

import re
from difflib import SequenceMatcher


def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", "", s.lower())).strip()


class SemanticMapper:
    def __init__(self, mapping_dictionary: dict[str, str], fuzzy_threshold: float = 0.86) -> None:
        self.mapping_dictionary = mapping_dictionary
        self.fuzzy_threshold = fuzzy_threshold
        self.normalized_dictionary = {_normalize(k): v for k, v in mapping_dictionary.items()}

    def resolve(self, label: str) -> dict[str, object]:
        if label in self.mapping_dictionary:
            return {"mapped_to": self.mapping_dictionary[label], "method": "dictionary", "confidence": 1.0}

        normalized = _normalize(label)
        if normalized in self.normalized_dictionary:
            return {
                "mapped_to": self.normalized_dictionary[normalized],
                "method": "normalized",
                "confidence": 0.95,
            }

        best_key = ""
        best_score = 0.0
        for dict_label in self.mapping_dictionary:
            score = SequenceMatcher(None, normalized, _normalize(dict_label)).ratio()
            if score > best_score:
                best_score = score
                best_key = dict_label

        if best_score >= self.fuzzy_threshold:
            return {
                "mapped_to": self.mapping_dictionary[best_key],
                "method": "fuzzy",
                "confidence": round(best_score, 2),
            }

        return {"mapped_to": None, "method": "unmapped", "confidence": 0.0}
