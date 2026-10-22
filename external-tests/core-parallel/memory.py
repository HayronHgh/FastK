"""Separate sampled memory/FD run. Do not use its perturbed query timings."""

import argparse
import json
import subprocess
import time
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("command", nargs=argparse.REMAINDER)
    args = p.parse_args()
    peak_rss = peak_private = peak_fds = samples = 0
    if Path("/proc").is_dir():

        def sample(pid):
            root = Path("/proc") / str(pid)
            fields = dict(
                line.split(":", 1)
                for line in (root / "status").read_text().splitlines()
                if ":" in line
            )
            rollup = dict(
                line.split(":", 1)
                for line in (root / "smaps_rollup").read_text().splitlines()
                if ":" in line
            )
            private = sum(
                int(rollup.get(key, "0").split()[0]) * 1024
                for key in ("Private_Clean", "Private_Dirty", "Private_Hugetlb")
            )
            return (
                int(fields.get("VmRSS", "0").split()[0]) * 1024,
                private,
                len(list((root / "fd").iterdir())),
            )

        private_kind = "USS"
    else:
        import psutil

        def sample(pid):
            try:
                info = psutil.Process(pid).memory_info()
            except psutil.NoSuchProcess:
                raise ProcessLookupError from None
            return info.rss, info.private, 0

        private_kind = "private_commit"
    with args.output.with_suffix(".raw.json").open("wb") as out:
        child = subprocess.Popen(args.command, stdout=out)
        while child.poll() is None:
            try:
                rss, private, fds = sample(child.pid)
                peak_rss, peak_private, peak_fds = (
                    max(peak_rss, rss),
                    max(peak_private, private),
                    max(peak_fds, fds),
                )
                samples += 1
            except (OSError, ProcessLookupError):
                pass
            time.sleep(0.005)
        assert child.wait() == 0
    raw = json.loads(args.output.with_suffix(".raw.json").read_text())
    assert raw["all_results_verified"]
    args.output.write_text(
        json.dumps(
            dict(
                pid=child.pid,
                command=args.command,
                samples=samples,
                peak_rss=peak_rss,
                peak_private=peak_private,
                private_kind=private_kind,
                peak_fds=peak_fds,
                fd_sampling_supported=private_kind == "USS",
                interval_ms=5,
                timings_perturbed=True,
                all_results_verified=True,
            ),
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
