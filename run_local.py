from pathlib import Path
from financial_digitization.pipelines.job_runner import ETLJobRunner

INPUT_PATH = Path(r"D:\digitize-data\pdf")
OUTPUT_ROOT = Path(r"D:\digitize-data\output")

def collect_pdfs(p: Path) -> list[Path]:
    if p.is_file() and p.suffix.lower() == ".pdf":
        return [p]
    if p.is_dir():
        return sorted(p.glob("*.pdf"))
    return []

if __name__ == "__main__":
    pdfs = collect_pdfs(INPUT_PATH)
    if not pdfs:
        raise SystemExit(f"No PDFs found at: {INPUT_PATH}")

    runner = ETLJobRunner(output_root=OUTPUT_ROOT)

    for pdf in pdfs:
        # NOTE: extraction is not implemented yet in this skeleton; pass placeholder page_texts
        envelope = runner.run(file_paths=[pdf], page_texts=[""])
        job_id = envelope["job"]["job_id"]
        job_dir = OUTPUT_ROOT / job_id
        print(f"OK: {pdf.name} -> {job_dir}")
