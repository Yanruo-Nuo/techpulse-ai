"""Periodic OSS → MaxCompute → dbt sync (runs every 5 minutes)"""

import subprocess
import time
import sys
import os

from prometheus_client import start_http_server
from metrics import mc_sync_duration_seconds, mc_sync_rows, mc_sync_success, dbt_run_duration_seconds, dbt_run_success

SCRIPT = os.path.join(os.path.dirname(__file__), "oss_to_mc_runner.py")
INTERVAL = 300  # 5 minutes


def run():
    start_http_server(8004)
    print("Metrics HTTP server started on port 8004")
    while True:
        print(f"[{time.strftime('%H:%M:%S')}] Syncing OSS → MaxCompute → dbt...")
        _sync_start = time.time()
        result = subprocess.run(
            [sys.executable, SCRIPT],
            capture_output=True,
            text=True,
        )
        _sync_dur = time.time() - _sync_start

        for line in result.stdout.splitlines():
            if any(x in line for x in ["Found", "Total", "done", "Error", "OK", "FAIL", "PASS"]):
                print(f"  {line}")

        if result.returncode == 0:
            mc_sync_success.set(1)
            mc_sync_duration_seconds.set(_sync_dur)
            dbt_run_success.set(1)

            for line in result.stdout.splitlines():
                if "Total records:" in line:
                    try:
                        mc_sync_rows.set(int(line.split(":")[1].strip()))
                    except ValueError:
                        pass
        else:
            mc_sync_success.set(0)
            dbt_run_success.set(0)
            print(f"  Error: {result.stderr[-300:]}")

        print(f"[{time.strftime('%H:%M:%S')}] Sleep {INTERVAL}s...")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    run()
