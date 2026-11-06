"""Run the server: python -m agent2grokbot [port] [--host HOST]."""

import argparse

import uvicorn

from .app import create_app


def main():
    parser = argparse.ArgumentParser(prog="agent2grokbot")
    parser.add_argument("port", nargs="?", type=int, default=7583)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()
    uvicorn.run(create_app(), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
