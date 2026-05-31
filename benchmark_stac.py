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
from typing import Any, Optional

import aiohttp

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("benchmark")


# Minimal config -> will be part of config.py
@dataclass
class BenchmarkConfig:
    sta_base_url: str = "https://airquality-frost.k8s.ilt-dmz.iosb.fraunhofer.de/v1.1"
    stac_root_href: str = "http://localhost:8020/stac"
    page_size: int = 100
    timeout_seconds: float = 30.0
    output_path: str = "benchmark_output.json"


# Inline data models -> will be part of harvester.py
@dataclass
class HarvestedThing:
    id: int
    self_link: str
    name: str
    description: Optional[str]
    properties: Optional[dict]
    locations: list[dict]
    datastreams: list[dict]


@dataclass
class HarvestedCatalog:
    base_url: str
    conformance: list[str]
    things: list[HarvestedThing]
    harvested_at: str
    thing_count: int = field(init=False)

    def __post_init__(self) -> None:
        self.thing_count = len(self.things)
