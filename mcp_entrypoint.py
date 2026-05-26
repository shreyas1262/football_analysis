import os
import uvicorn
from football_analytics.mcp.server import create_app

if __name__ == "__main__":
    uvicorn.run(create_app(), host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
