import subprocess
import sys
from pathlib import Path

import webants

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_core_types_are_exported():
    for name in (
        "Spider",
        "Request",
        "Result",
        "Field",
        "Item",
        "AttrItem",
        "LinkItem",
        "ElementItem",
        "TextItem",
    ):
        assert hasattr(webants, name)
        assert name in webants.__all__


def test_root_import_does_not_load_optional_dependencies():
    code = "import webants, sys; print('playwright' in sys.modules, 'psutil' in sys.modules)"
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "False False"
