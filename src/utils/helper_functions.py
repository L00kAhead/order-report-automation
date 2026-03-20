import json
from io import BytesIO
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

BASE_DIR = Path(__file__).resolve().parents[1]

def _policy_path(filename: str) -> Path:
    return BASE_DIR / 'policies' / filename


def load_json_policy(filename: str) -> dict:
    with _policy_path(filename).open('r', encoding='utf-8') as policy_file:
        return json.load(policy_file)

def build_lambda_package() -> bytes:
    source_path = BASE_DIR / 'lambda_function.py'
    buffer = BytesIO()

    with ZipFile(buffer, 'w', compression=ZIP_DEFLATED) as zip_file:
        zip_file.write(source_path, arcname='lambda_function.py')

    return buffer.getvalue()