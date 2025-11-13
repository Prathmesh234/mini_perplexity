import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from cc_download_script.cc_download import download_cc_wet_to_azure


def _str_to_int(value: Optional[str], default: int) -> int:
    if value is None or value.strip() == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def main():
    # Auto-load .env from the data directory
    env_path = Path(__file__).with_name(".env")
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)

    azure_conn_str = os.getenv("AZURE_CONN_STR", "")
    if not azure_conn_str:
        raise RuntimeError("AZURE_CONN_STR env var is required")

    container_name = os.getenv("CONTAINER_NAME", "commoncrawl-wet")
    index_url = os.getenv(
        "INDEX_URL",
        "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-40/wet.paths.gz",
    )
    target_mb = _str_to_int(os.getenv("TARGET_MB"), 5 * 1024)
    avg_file_mb = _str_to_int(os.getenv("AVG_FILE_MB"), 100)
    seed_env = os.getenv("SEED")
    seed = int(seed_env) if seed_env is not None and seed_env.strip() != "" else None
    min_english_files = _str_to_int(os.getenv("MIN_ENGLISH_FILES"), 0)

    summary = download_cc_wet_to_azure(
        azure_connection_string=azure_conn_str,
        container_name=container_name,
        index_url=index_url,
        target_mb=target_mb,
        avg_file_mb=avg_file_mb,
        seed=seed,
        min_english_files=min_english_files,
    )
    print(summary)


if __name__ == "__main__":
    main()
