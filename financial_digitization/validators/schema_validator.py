from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    from jsonschema import Draft202012Validator  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    Draft202012Validator = None


class SchemaValidator:
    def __init__(self, schema_dir: Path | None = None) -> None:
        self.schema_dir = schema_dir or Path(__file__).resolve().parents[1] / "schemas" / "1.0.0"

    def _load_schema(self, schema_name: str) -> dict[str, Any]:
        schema_path = self.schema_dir / schema_name
        return json.loads(schema_path.read_text(encoding="utf-8"))

    def validate(self, payload: dict[str, Any], schema_name: str) -> list[str]:
        schema = self._load_schema(schema_name)
        if Draft202012Validator is not None:
            validator = Draft202012Validator(schema)
            errors = sorted(validator.iter_errors(payload), key=lambda e: e.path)
            return [f"{'/'.join(map(str, err.path))}: {err.message}" for err in errors]
        return _fallback_validate(payload, schema)


def _fallback_validate(payload: Any, schema: dict[str, Any], path: str = "") -> list[str]:
    errors: list[str] = []
    expected_type = schema.get("type")
    if expected_type == "object":
        if not isinstance(payload, dict):
            return [f"{path}: Expected object"]
        required = schema.get("required", [])
        for key in required:
            if key not in payload:
                errors.append(f"{path}: '{key}' is a required property")

        props = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            for key in payload:
                if key not in props:
                    errors.append(f"{path}: Additional properties are not allowed ('{key}' was unexpected)")

        for key, value in payload.items():
            child_path = f"{path}/{key}" if path else key
            if key in props:
                errors.extend(_fallback_validate(value, props[key], child_path))
            elif isinstance(schema.get("additionalProperties"), dict):
                errors.extend(_fallback_validate(value, schema["additionalProperties"], child_path))

    elif expected_type == "array":
        if not isinstance(payload, list):
            return [f"{path}: Expected array"]
        item_schema = schema.get("items")
        if item_schema:
            for idx, item in enumerate(payload):
                errors.extend(_fallback_validate(item, item_schema, f"{path}[{idx}]"))

    elif expected_type == "string" and not isinstance(payload, str):
        errors.append(f"{path}: Expected string")
    elif expected_type == "integer" and not isinstance(payload, int):
        errors.append(f"{path}: Expected integer")
    elif expected_type == "number" and not isinstance(payload, (int, float)):
        errors.append(f"{path}: Expected number")
    elif expected_type == "boolean" and not isinstance(payload, bool):
        errors.append(f"{path}: Expected boolean")

    const = schema.get("const")
    if const is not None and payload != const:
        errors.append(f"{path}: Value must be {const}")

    enum = schema.get("enum")
    if enum is not None and payload not in enum:
        errors.append(f"{path}: Value must be one of {enum}")

    for key in ("$defs", "$schema", "title", "minProperties", "minimum", "maximum"):
        _ = schema.get(key)
    return errors
