"""
STAC Transformation Benchmark

Fetches raw metadata from a live istSOS4 SensorThings API instance,
runs it through the STAC transformer, times the transformation in isolation,
and saves the output to a JSON file.

Network I/O (fetch) and transformation are timed separately so the
benchmark result reflects only transformation cost.

Usage:
    python benchmark_stac.py

Output:
    - JSON file with the full STAC catalog (default: benchmark_output.json)
    - Timing summary printed to stdout
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import aiohttp

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("benchmark")
