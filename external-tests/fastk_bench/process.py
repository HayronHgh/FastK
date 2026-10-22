"""Bounded worker execution with memory sampled from the actual Python PID."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import psutil


def announce(config):
    Path(config["pid_file"]).write_text(str(os.getpid()))


def run(module, config_path, output, timeout):
    config = json.loads(config_path.read_text())
    pid_file = output.with_suffix(".pid")
    config["pid_file"] = str(pid_file)
    config_path.write_text(json.dumps(config) + "\n")
    log = output.with_suffix(".log")
    peak_rss = peak_private = peak_threads = samples = 0
    actual = None
    child_peaks = {}
    tree_peak_rss = 0
    with log.open("wb") as stream:
        environment = os.environ.copy()
        for name in [
            "OPENBLAS_NUM_THREADS",
            "OMP_NUM_THREADS",
            "MKL_NUM_THREADS",
            "NUMEXPR_NUM_THREADS",
        ]:
            environment[name] = "1"
        child = subprocess.Popen(
            [sys.executable, "-m", module, str(config_path)],
            cwd=Path(__file__).resolve().parents[1],
            stdout=stream,
            stderr=subprocess.STDOUT,
            env=environment,
        )
        launcher = psutil.Process(child.pid)
        deadline = time.monotonic() + timeout
        try:
            while child.poll() is None:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"worker exceeded {timeout}s; see {log}")
                if actual is None and pid_file.is_file():
                    try:
                        pid = int(pid_file.read_text())
                        allowed = {child.pid, *(p.pid for p in launcher.children(recursive=True))}
                        if pid not in allowed:
                            raise RuntimeError("worker PID is outside its process tree")
                        actual = psutil.Process(pid)
                    except ValueError:
                        pass  # A tiny PID file can be observed before write completion.
                if actual is not None:
                    try:
                        memory = actual.memory_info()
                        private = getattr(memory, "private", None)
                        if private is None:
                            private = actual.memory_full_info().uss
                        peak_rss = max(peak_rss, memory.rss)
                        peak_private = max(peak_private, private)
                        peak_threads = max(peak_threads, actual.num_threads())
                        samples += 1
                        if config.get("monitor_children"):
                            tree_rss = memory.rss
                            for descendant in actual.children(recursive=True):
                                try:
                                    info = descendant.memory_info()
                                    child_peaks[descendant.pid] = max(
                                        child_peaks.get(descendant.pid, 0), info.rss
                                    )
                                    tree_rss += info.rss
                                except psutil.NoSuchProcess:
                                    pass
                            tree_peak_rss = max(tree_peak_rss, tree_rss)
                    except psutil.NoSuchProcess:
                        pass
                time.sleep(0.005)
        finally:
            if child.poll() is None:
                for descendant in launcher.children(recursive=True):
                    try:
                        descendant.kill()
                    except psutil.NoSuchProcess:
                        pass
                child.kill()
            child.wait()
        if child.returncode:
            raise RuntimeError(f"worker failed: {log}\n{log.read_text(errors='replace')[-4000:]}")
    result = json.loads(output.read_text())
    if actual is None or result["python_pid"] != actual.pid or not samples:
        raise AssertionError("actual Python process was not sampled")
    result["memory"] = {
        "python_pid": actual.pid,
        "peak_rss_bytes": peak_rss,
        "peak_private_bytes": peak_private,
        "private_definition": "Windows private commit; Linux unique resident set (USS)",
        "sampling_interval_ms": 5,
        "samples": samples,
        "peak_process_thread_count": peak_threads,
        "numeric_library_thread_limits": 1,
        "scope": "actual worker after bootstrap, includes backend imports and validation; sampled peaks are lower bounds",
    }
    if config.get("monitor_children"):
        result["memory"]["child_peak_rss_bytes_by_pid"] = child_peaks
        result["memory"]["peak_simultaneous_tree_rss_bytes"] = tree_peak_rss
    output.write_text(json.dumps(result, indent=2) + "\n")
    return result
