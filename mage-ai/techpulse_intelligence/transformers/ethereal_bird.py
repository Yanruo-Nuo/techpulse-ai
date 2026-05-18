import subprocess
import os

@transformer
def run_dbt(data, *args, **kwargs):
    print("🚀 Running dbt...")

    dbt_path = "/home/src/techpulse_dbt"

    if not os.path.exists(dbt_path):
        raise Exception(f"❌ dbt 项目路径不存在: {dbt_path}")

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = dbt_path

    result = subprocess.run(
        ["dbt-mc", "run"],   # ⭐ 用 dbt-mc
        cwd=dbt_path,
        env=env,
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise Exception("❌ dbt failed")

    print("✅ dbt 完成")

    return data