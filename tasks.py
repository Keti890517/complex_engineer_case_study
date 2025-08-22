#!/usr/bin/env python3
import os
import subprocess
import sys
from pathlib import Path

ENV_FILE = ".env"
DATA_DIR = Path("data")
OUTPUT_DIR = Path("output")
AIRFLOW_UID = "50000"
AIRFLOW_GID = "0"


def run(cmd, check=True, capture_output=False):
    """Run shell command in a cross-platform way."""
    print(f"▶ Running: {cmd}")
    result = subprocess.run(cmd, shell=True, check=check, capture_output=capture_output, text=True)
    return result.stdout.strip() if capture_output else None


def check_env():
    if not Path(ENV_FILE).exists():
        print("⚙️  .env not found, creating one...")
        api_key = input("Enter your OpenWeather API key: ").strip()
        with open(ENV_FILE, "w") as f:
            f.write(f"AIRFLOW_UID={AIRFLOW_UID}\n")
            f.write(f"AIRFLOW_GID={AIRFLOW_GID}\n")
            f.write(f"OPENWEATHER_API_KEY={api_key}\n")
        print(".env file created ✅")
    else:
        print("✅ Using existing .env")


def setup_dirs():
    DATA_DIR.mkdir(exist_ok=True, parents=True)
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    # Permissions only on Unix
    if os.name != "nt":
        run(f"sudo chown -R {AIRFLOW_UID}:{AIRFLOW_GID} {DATA_DIR} {OUTPUT_DIR}")
        run(f"sudo chmod -R 775 {DATA_DIR} {OUTPUT_DIR}")
    print("📂 Folders ready.")


def build():
    run("docker compose build")
    print("🔨 Docker images built.")


def check_db_initialized():
    """Return True if Airflow DB already has tables."""
    try:
        output = run(
            "docker compose exec -T postgres psql -U airflow -d airflow -c '\\dt'",
            capture_output=True,
        )
        # If tables exist, output is not empty
        return bool(output.strip())
    except subprocess.CalledProcessError:
        return False


def init():
    if check_db_initialized():
        print("✅ Airflow DB already initialized, skipping init.")
    else:
        print("🔧 Initializing Airflow database and users...")
        run("docker compose run --rm airflow-init")


def up():
    check_env()
    setup_dirs()
    build()
    init()
    run("docker compose up -d")
    print("🚀 Airflow stack started.")


def down():
    run("docker compose down")
    print("🛑 Airflow stack stopped.")


def restart():
    down()
    up()


def logs():
    run("docker compose logs -f")


def test():
    run("docker compose exec airflow-worker pytest tests/")
    print("🧪 Tests executed.")


# ------------------ CLI ------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Airflow ETL project tasks")
    parser.add_argument("command", choices=["up", "down", "restart", "logs", "test", "build", "init", "setup_dirs", "check_env"])
    args = parser.parse_args()

    COMMANDS = {
        "up": up,
        "down": down,
        "restart": restart,
        "logs": logs,
        "test": test,
        "build": build,
        "init": init,
        "setup_dirs": setup_dirs,
        "check_env": check_env,
    }

    COMMANDS[args.command]()