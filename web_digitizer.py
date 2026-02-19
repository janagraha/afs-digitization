from __future__ import annotations

import argparse
import re
import sys
import tempfile
import traceback
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

from financial_digitization.exporters.excel_writer import write_excel
from financial_digitization.extractors.pdf_tables import ExtractedTable, extract_page_texts, extract_tables
from financial_digitization.pipelines.job_runner import ETLJobRunner

_AMOUNT_TOKEN_RE = re.compile(r"(?<![A-Za-z0-9])[\(\-]?\d[\d,]*(?:\.\d+)?-?\)?(?![A-Za-z0-9])")
_YEAR_HEADER_RE = re.compile(r"(?:19|20)\d{2}(?:\s*-\s*(?:19|20)?\d{2})?|FY\s*\d{2,4}", re.IGNORECASE)
_ACCOUNT_CODE_PREFIX_BASE = 110000000
_ACCOUNT_CODE_STEP = 100


HTML_PAGE = """<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>PDF Digitization</title>
  <style>
    :root {
      --bg: linear-gradient(135deg, #f7fff6 0%, #eaf7f4 50%, #fdf3ea 100%);
      --ink: #11312d;
      --card: #ffffff;
      --accent: #0c8f6b;
      --muted: #48625d;
      --danger: #a1273f;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      font-family: "Segoe UI", Tahoma, sans-serif;
      color: var(--ink);
      background: var(--bg);
      padding: 20px;
    }
    .card {
      width: min(640px, 100%);
      background: var(--card);
      border-radius: 16px;
      padding: 28px;
      box-shadow: 0 18px 44px rgba(8, 47, 39, 0.14);
    }
    h1 {
      margin: 0 0 10px;
      font-size: clamp(1.3rem, 2.8vw, 1.9rem);
    }
    p {
      margin: 0 0 20px;
      color: var(--muted);
    }
    .row {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
    }
    input[type=file] {
      border: 1px solid #cadcd8;
      border-radius: 10px;
      padding: 8px;
      flex: 1 1 300px;
      background: #f9fffe;
    }
    button {
      border: none;
      border-radius: 10px;
      padding: 10px 16px;
      font-size: 0.95rem;
      font-weight: 600;
      background: var(--accent);
      color: #fff;
      cursor: pointer;
      transition: transform 0.2s ease;
    }
    button:disabled {
      opacity: 0.65;
      cursor: not-allowed;
    }
    button:not(:disabled):hover { transform: translateY(-1px); }
    #status {
      margin-top: 14px;
      min-height: 1.2em;
      font-size: 0.95rem;
      color: var(--muted);
    }
    #status.error { color: var(--danger); }
  </style>
</head>
<body>
  <main class=\"card\">
    <h1>Financial PDF Digitization</h1>
    <p>Select a PDF, run digitization, and download the resulting Excel file.</p>
    <div class=\"row\">
      <input id=\"pdfFile\" type=\"file\" accept=\"application/pdf,.pdf\" />
      <button id=\"uploadBtn\" type=\"button\">Digitize & Download Excel</button>
    </div>
    <div id=\"status\"></div>
  </main>

  <script>
    const fileInput = document.getElementById('pdfFile');
    const button = document.getElementById('uploadBtn');
    const statusEl = document.getElementById('status');

    const setStatus = (message, isError = false) => {
      statusEl.textContent = message;
      statusEl.className = isError ? 'error' : '';
    };

    button.addEventListener('click', async () => {
      const file = fileInput.files[0];
      if (!file) {
        setStatus('Please choose a PDF file first.', true);
        return;
      }

      if (!file.name.toLowerCase().endsWith('.pdf')) {
        setStatus('Only .pdf files are allowed.', true);
        return;
      }

      button.disabled = true;
      setStatus('Digitizing...');

      try {
        const response = await fetch('/digitize', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/pdf',
            'X-Filename': encodeURIComponent(file.name)
          },
          body: file,
        });

        if (!response.ok) {
          const message = await response.text();
          throw new Error(message || 'Digitization failed');
        }

        const blob = await response.blob();
        const disposition = response.headers.get('Content-Disposition') || '';
        const nameMatch = disposition.match(/filename=\"([^\"]+)\"/i);
        const outName = nameMatch ? nameMatch[1] : 'digitized_output.xlsx';

        const url = window.URL.createObjectURL(blob);
        const anchor = document.createElement('a');
        anchor.href = url;
        anchor.download = outName;
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
        window.URL.revokeObjectURL(url);

        setStatus('Digitization complete. Download started.');
      } catch (error) {
        setStatus(error.message || 'Digitization failed.', true);
      } finally {
        button.disabled = false;
      }
    });
  </script>
</body>
</html>
"""


class DigitizationHandler(BaseHTTPRequestHandler):
    server_version = "AFSDigitizer/1.0"

    def _send_html(self, body: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _send_text(self, body: str, status: HTTPStatus) -> None:
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: object) -> None:
        """Avoid connection drops when stderr is unavailable in some IDE run modes."""
        try:
            message = "%s - - [%s] %s\n" % (
                self.client_address[0],
                self.log_date_time_string(),
                format % args,
            )
            sys.stdout.write(message)
        except Exception:
            pass

    def do_GET(self) -> None:  # noqa: N802
        try:
            if urlparse(self.path).path == "/":
                self._send_html(HTML_PAGE)
                return
            self._send_text("Not Found", HTTPStatus.NOT_FOUND)
        except Exception:  # pragma: no cover - defensive error response
            traceback.print_exc()
            self._send_text("Server error while serving page", HTTPStatus.INTERNAL_SERVER_ERROR)

    @staticmethod
    def _flatten_to_rows(payload: object) -> list[list[object]]:
        rows: list[list[object]] = [["Field", "Value"]]

        def walk(prefix: str, value: object) -> None:
            if isinstance(value, dict):
                if not value:
                    rows.append([prefix, ""])
                    return
                for key, nested in value.items():
                    next_prefix = f"{prefix}.{key}" if prefix else key
                    walk(next_prefix, nested)
                return
            if isinstance(value, list):
                if not value:
                    rows.append([prefix, "[]"])
                    return
                for idx, nested in enumerate(value):
                    walk(f"{prefix}[{idx}]", nested)
                return
            rows.append([prefix, "" if value is None else value])

        walk("", payload)
        return rows

    @staticmethod
    def _tables_to_rows(tables: list[ExtractedTable], page_texts: list[str]) -> list[list[object]]:
        default_rows: list[list[object]] = [
            ["Account Code", "Particulars", "Current Year Amount", "Previous Year Amount"]
        ]

        if tables:
            # Use the first parsed table to determine output headers.
            first_table_rows = DigitizationHandler._normalize_table_rows(tables[0].rows)
            header_row_idx = DigitizationHandler._detect_header_row(first_table_rows)
            table_title = DigitizationHandler._infer_table_title(tables[0], first_table_rows, header_row_idx, page_texts)
            particulars_header, current_label, previous_label = DigitizationHandler._derive_column_labels(
                first_table_rows,
                header_row_idx,
                table_title,
            )

            rows: list[list[object]] = [
                [
                    "Account Code",
                    particulars_header,
                    f"Current Year Amount ({current_label})",
                    f"Previous Year Amount ({previous_label})",
                ]
            ]

            generated_index = 0
            for table in tables:
                parsed_rows = DigitizationHandler._table_data_rows(table)
                for particulars, current_amount, previous_amount in parsed_rows:
                    if not particulars and not current_amount and not previous_amount:
                        continue
                    account_code, cleaned_particulars = DigitizationHandler._extract_account_code(particulars)
                    if not account_code:
                        account_code = str(_ACCOUNT_CODE_PREFIX_BASE + (generated_index * _ACCOUNT_CODE_STEP))
                    generated_index += 1
                    rows.append([account_code, cleaned_particulars, current_amount, previous_amount])

            if len(rows) > 1:
                return rows

        generated_index = 0
        for page_text in page_texts:
            for raw_line in page_text.splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                particulars, current_amount, previous_amount = DigitizationHandler._split_particulars_and_amounts([line])
                if not particulars and not current_amount and not previous_amount:
                    continue
                account_code, cleaned_particulars = DigitizationHandler._extract_account_code(particulars)
                if not account_code:
                    account_code = str(_ACCOUNT_CODE_PREFIX_BASE + (generated_index * _ACCOUNT_CODE_STEP))
                generated_index += 1
                default_rows.append([account_code, cleaned_particulars, current_amount, previous_amount])

        if len(default_rows) == 1:
            default_rows.append([str(_ACCOUNT_CODE_PREFIX_BASE), "No extractable text/table found in PDF", "", ""])
        return default_rows

    @staticmethod
    def _normalize_table_rows(rows: list[list[str]]) -> list[list[str]]:
        normalized: list[list[str]] = []
        for row in rows:
            clean_row = [cell.strip() for cell in row if cell and cell.strip()]
            if clean_row:
                normalized.append(clean_row)
        return normalized

    @staticmethod
    def _table_data_rows(table: ExtractedTable) -> list[tuple[str, str, str]]:
        normalized_rows = DigitizationHandler._normalize_table_rows(table.rows)
        if not normalized_rows:
            return []

        header_row_idx = DigitizationHandler._detect_header_row(normalized_rows)
        data_start = (header_row_idx + 1) if header_row_idx is not None else 0

        parsed: list[tuple[str, str, str]] = []
        for row in normalized_rows[data_start:]:
            parsed.append(DigitizationHandler._split_particulars_and_amounts(row))
        return parsed

    @staticmethod
    def _detect_header_row(rows: list[list[str]]) -> int | None:
        for idx, row in enumerate(rows[:3]):
            if len(row) < 2:
                continue
            first = row[0].lower()
            if "particular" in first or "description" in first or "head" in first:
                return idx
            period_like = sum(1 for cell in row[1:] if _YEAR_HEADER_RE.search(cell))
            if period_like >= 1:
                return idx
        return None

    @staticmethod
    def _derive_column_labels(
        rows: list[list[str]],
        header_row_idx: int | None,
        table_title: str,
    ) -> tuple[str, str, str]:
        particulars_label = table_title or "Particulars"
        current_label = "Current Year"
        previous_label = "Previous Year"

        if header_row_idx is None or header_row_idx >= len(rows):
            return particulars_label, current_label, previous_label

        header = rows[header_row_idx]
        if header and header[0].strip():
            particulars_label = header[0].strip()

        period_labels = [cell.strip() for cell in header[1:] if cell.strip()]
        if len(period_labels) >= 2:
            current_label = period_labels[-2]
            previous_label = period_labels[-1]
        elif len(period_labels) == 1:
            current_label = period_labels[0]

        return particulars_label, current_label, previous_label

    @staticmethod
    def _infer_table_title(
        table: ExtractedTable,
        rows: list[list[str]],
        header_row_idx: int | None,
        page_texts: list[str],
    ) -> str:
        page_idx = table.page - 1
        if page_idx < 0 or page_idx >= len(page_texts):
            return "Particulars"

        lines = [line.strip() for line in page_texts[page_idx].splitlines() if line.strip()]
        if not lines:
            return "Particulars"

        anchor = ""
        if header_row_idx is not None and header_row_idx < len(rows) and rows[header_row_idx]:
            anchor = rows[header_row_idx][0].strip()

        if anchor:
            for idx, line in enumerate(lines):
                if anchor.lower() in line.lower():
                    for back in range(idx - 1, -1, -1):
                        if lines[back]:
                            return lines[back]
                    break

        return lines[0]

    @staticmethod
    def _extract_account_code(particulars: str) -> tuple[str, str]:
        match = re.match(r"^\s*(\d{6,12})\s+(.+)$", particulars)
        if not match:
            return "", particulars
        return match.group(1), match.group(2).strip()

    @staticmethod
    def _looks_like_amount(token: str) -> bool:
        stripped = token.strip()
        digits = "".join(ch for ch in stripped if ch.isdigit())
        if not digits:
            return False
        if "," in stripped or "." in stripped or stripped.endswith("-") or stripped.startswith("(") or stripped.endswith(")"):
            return True
        if len(digits) == 4:
            year_value = int(digits)
            if 1900 <= year_value <= 2099:
                return False
        return stripped.isdigit()

    @staticmethod
    def _extract_amount_tokens(text: str) -> list[str]:
        return [token for token in _AMOUNT_TOKEN_RE.findall(text) if DigitizationHandler._looks_like_amount(token)]

    @staticmethod
    def _pop_trailing_amount(text: str) -> tuple[str, str]:
        match = re.search(r"(?<![A-Za-z0-9])([\(\-]?\d[\d,]*(?:\.\d+)?-?\)?)[\s]*$", text)
        if not match:
            return text, ""
        token = match.group(1)
        if not DigitizationHandler._looks_like_amount(token):
            return text, ""
        return text[: match.start()].rstrip(), token

    @staticmethod
    def _is_amount_only_cell(cell: str) -> bool:
        tokens = DigitizationHandler._extract_amount_tokens(cell)
        if len(tokens) != 1:
            return False
        leftover = cell.replace(tokens[0], "", 1).strip().lower()
        return leftover in {"", "rs", "inr", "dr", "cr"}

    @staticmethod
    def _split_particulars_and_amounts(row: list[str]) -> tuple[str, str, str]:
        if not row:
            return "", "", ""

        clean_cells = [cell.strip() for cell in row if cell and cell.strip()]
        if not clean_cells:
            return "", "", ""

        particulars_parts = [clean_cells[0]]
        detected_amounts: list[str] = []

        for cell in clean_cells[1:]:
            if DigitizationHandler._is_amount_only_cell(cell):
                detected_amounts.extend(DigitizationHandler._extract_amount_tokens(cell))
            else:
                particulars_parts.append(cell)

        particulars_text = " ".join(particulars_parts).strip()

        # If the current-year amount is attached to the particulars cell, detach it.
        trailing_amounts: list[str] = []
        while True:
            updated_text, token = DigitizationHandler._pop_trailing_amount(particulars_text)
            if not token:
                break
            trailing_amounts.insert(0, token)
            particulars_text = updated_text

        all_amounts = trailing_amounts + detected_amounts
        current_amount = ""
        previous_amount = ""
        if len(all_amounts) >= 2:
            current_amount = all_amounts[-2]
            previous_amount = all_amounts[-1]
        elif len(all_amounts) == 1:
            current_amount = all_amounts[0]

        return particulars_text.strip(), current_amount, previous_amount

    def do_POST(self) -> None:  # noqa: N802
        try:
            if urlparse(self.path).path != "/digitize":
                self._send_text("Not Found", HTTPStatus.NOT_FOUND)
                return

            length_header = self.headers.get("Content-Length")
            if not length_header:
                self._send_text("Missing Content-Length", HTTPStatus.BAD_REQUEST)
                return

            content_type = (self.headers.get("Content-Type") or "").lower()
            if "application/pdf" not in content_type:
                self._send_text("Content-Type must be application/pdf", HTTPStatus.BAD_REQUEST)
                return

            try:
                content_length = int(length_header)
            except ValueError:
                self._send_text("Invalid Content-Length", HTTPStatus.BAD_REQUEST)
                return

            pdf_data = self.rfile.read(content_length)
            if not pdf_data.startswith(b"%PDF"):
                self._send_text("Uploaded content is not a valid PDF", HTTPStatus.BAD_REQUEST)
                return

            filename_header = unquote(self.headers.get("X-Filename", "uploaded.pdf"))
            safe_name = Path(filename_header).name
            if not safe_name.lower().endswith(".pdf"):
                safe_name += ".pdf"

            with tempfile.TemporaryDirectory(prefix="afs_digitize_") as temp_root:
                temp_path = Path(temp_root)
                pdf_path = temp_path / safe_name
                output_root = temp_path / "out"
                pdf_path.write_bytes(pdf_data)

                page_texts = extract_page_texts(pdf_path)
                extracted_tables = extract_tables(pdf_path, page_texts=page_texts)

                runner = ETLJobRunner(output_root=output_root)
                runner.run(file_paths=[pdf_path], page_texts=page_texts or [""])

                out_name = f"{pdf_path.stem}_digitized.xlsx"
                excel_path = temp_path / out_name
                write_excel(excel_path, self._tables_to_rows(extracted_tables, page_texts))
                excel_bytes = excel_path.read_bytes()

                self.send_response(HTTPStatus.OK)
                self.send_header(
                    "Content-Type",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
                self.send_header("Content-Disposition", f'attachment; filename="{out_name}"')
                self.send_header("Content-Length", str(len(excel_bytes)))
                self.end_headers()
                self.wfile.write(excel_bytes)
        except Exception as exc:  # pragma: no cover - defensive error response
            traceback.print_exc()
            self._send_text(f"Digitization failed: {exc}", HTTPStatus.INTERNAL_SERVER_ERROR)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run local web uploader for PDF digitization.")
    parser.add_argument("--host", default="127.0.0.1", help="Host address to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on (default: 8080)")
    args = parser.parse_args(argv)

    server = ThreadingHTTPServer((args.host, args.port), DigitizationHandler)
    print(f"Serving on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
