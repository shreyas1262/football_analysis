import json
import os

import uvicorn
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import TextContent, Tool
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from football_analytics.agent.tool_handlers import ToolHandlers

server = Server("football-analytics")


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name=s["name"],
            description=s["description"],
            inputSchema=s["input_schema"],
        )
        for s in ToolHandlers.SCHEMAS
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    handler = getattr(ToolHandlers, name, None)
    if handler is None:
        result = json.dumps({"error": f"Unknown tool: {name}"})
    else:
        result = json.dumps(handler(**arguments), default=str)

    return [TextContent(type="text", text=result)]


# ---------------------------------------------------------------------------
# HTTP/SSE transport
# ---------------------------------------------------------------------------

def create_app() -> Starlette:
    sse = SseServerTransport("/messages/")

    async def handle_sse(request):
        async with sse.connect_sse(
            request.scope,
            request.receive,
            request._send,
        ) as streams:
            await server.run(
                streams[0],
                streams[1],
                server.create_initialization_options(),
            )

    async def handle_messages(request):
        return await sse.handle_post_message(
            request.scope,
            request.receive,
            request._send,
        )

    async def health_check(request):
        return JSONResponse({"status": "ok", "server": "football-analytics"})

    return Starlette(
        routes=[
            Route("/sse", endpoint=handle_sse),
            Route("/health", endpoint=health_check),
            Route("/messages/", endpoint=handle_messages, methods=["POST"]),
        ]
    )


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(create_app(), host="0.0.0.0", port=port)
