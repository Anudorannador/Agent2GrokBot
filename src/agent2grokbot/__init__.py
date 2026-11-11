"""agent2grokbot: an async FastAPI MCP server bridging MCP clients to webhook peers."""

__version__ = "0.4.0"

from .app import create_app  # noqa: E402

__all__ = ["create_app"]
