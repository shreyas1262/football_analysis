"""
Uvicorn entrypoint for the football MCP server.

Exists solely to give uvicorn a module string it can import without hitting the
local mcp/ vs installed mcp-sdk namespace conflict. Loads football_mcp_server.py
by file path so the mcp package name never enters the import machinery here.
"""
import importlib.util
import sys
from pathlib import Path

_root = Path(__file__).parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

_spec = importlib.util.spec_from_file_location(
    "football_mcp_server",
    str(_root / "mcp" / "football_mcp_server.py"),
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

create_app = _mod.create_app
