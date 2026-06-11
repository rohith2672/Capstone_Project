from pathlib import Path

def replace_in_file(path_str):
    path = Path(path_str)
    if not path.exists():
        return
    text = path.read_text(encoding="utf-8")
    text = text.replace("write_parquet", "write_csv")
    text = text.replace("read_parquet", "read_csv")
    text = text.replace(".parquet", ".csv")
    text = text.replace("to_parquet", "to_csv")
    text = text.replace("parquet_format", "csv_format")
    path.write_text(text, encoding="utf-8")

files_to_update = [
    "etl/processor.py",
    "tests/test_processor_pipeline.py",
    "tests/test_storage.py"
]

for f in files_to_update:
    replace_in_file(f)
    print(f"Updated {f}")
