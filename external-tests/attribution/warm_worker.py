"""Explicit extra control: prewarm all query keys on the same handles before timing."""

import json
import sys
from pathlib import Path

from attribution import worker


def main():
    config = json.loads(Path(sys.argv[1]).read_text())
    original = worker.open_route
    queries = json.loads(Path(config["short_queries"]).read_text())

    def prewarmed(config, manifest):
        call, close = original(config, manifest)
        for q in queries:
            batch, _ = call(q)
            del batch
        return call, close

    worker.open_route = prewarmed
    worker.main()


if __name__ == "__main__":
    main()
