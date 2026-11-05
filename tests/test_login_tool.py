"""Static checks for the optional Playwright login helpers (P2-07).

The module imports Playwright at import time, so these tests inspect the AST
instead of importing it. This keeps the suite offline and independent of the
optional extra.
"""

import ast
from pathlib import Path

SOURCE_PATH = Path(__file__).resolve().parents[1] / "webants" / "utils" / "login.py"


def _function(tree: ast.Module, name: str) -> ast.AST:
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    raise AssertionError(f"{name} not found")


def _time_sleep_calls(node: ast.AST) -> list[ast.Call]:
    calls = []
    for child in ast.walk(node):
        if not isinstance(child, ast.Call):
            continue
        func = child.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr == "sleep"
            and isinstance(func.value, ast.Name)
            and func.value.id == "time"
        ):
            calls.append(child)
    return calls


def test_async_login_uses_asyncio_sleep():
    tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    function = _function(tree, "async_login_with_playwright")
    assert _time_sleep_calls(function) == []


def test_login_helpers_accept_sample_url():
    tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    for name in ("async_login_with_playwright", "sync_login_with_playwright"):
        function = _function(tree, name)
        args = [arg.arg for arg in function.args.args + function.args.kwonlyargs]
        assert "sample_url" in args
