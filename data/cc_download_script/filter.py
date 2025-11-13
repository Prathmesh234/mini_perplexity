"""Simple language filter for Common Crawl WET files."""

import gzip
import io

from langdetect import detect, LangDetectException


def is_english_wet_file(wet_gzip_bytes: bytes) -> bool:
    """Return True when the provided gzipped WET payload appears to be English."""

    try:
        with gzip.GzipFile(fileobj=io.BytesIO(wet_gzip_bytes)) as gz:
            sample = gz.read(200_000)
    except OSError:
        return False

    text = sample.decode("utf-8", errors="ignore").strip()
    if not text:
        return False

    try:
        return detect(text) == "en"
    except LangDetectException:
        return False
