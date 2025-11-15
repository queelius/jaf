#!/usr/bin/env python3
"""
Script to run the JAF FastAPI server.

Usage:
    python scripts/run_api.py [--host HOST] [--port PORT] [--reload]
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path to import jaf
sys.path.insert(0, str(Path(__file__).parent.parent))


def main():
    parser = argparse.ArgumentParser(description="Run JAF FastAPI server")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to (default: 8000)")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")
    parser.add_argument("--workers", type=int, default=1, help="Number of worker processes")
    
    args = parser.parse_args()
    
    try:
        import uvicorn
    except ImportError:
        print("Error: uvicorn not installed. Install with: pip install uvicorn[standard]")
        sys.exit(1)
    
    try:
        import fastapi
    except ImportError:
        print("Error: fastapi not installed. Install with: pip install fastapi")
        sys.exit(1)
    
    print(f"Starting JAF API server on http://{args.host}:{args.port}")
    print("API documentation available at: http://{args.host}:{args.port}/docs")
    print("Press CTRL+C to stop the server")
    
    uvicorn.run(
        "jaf.api:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        workers=args.workers if not args.reload else 1
    )


if __name__ == "__main__":
    main()