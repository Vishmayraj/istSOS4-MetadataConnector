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


# Fetch helpers -> basic utility functions
async def _fetch_json(session: aiohttp.ClientSession, url: str) -> dict[str, Any]:
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.json(content_type=None)


async def _paginate_things(
    session: aiohttp.ClientSession,
    initial_url: str,
) -> list[dict]:
    raw_things: list[dict] = []
    url: Optional[str] = initial_url
    page = 0

    while url:
        payload = await _fetch_json(session, url)
        items = payload.get("value", [])
        raw_things.extend(items)
        page += 1
        url = payload.get("@iot.nextLink") or None
        logger.info("Fetched page %d -- %d Things so far", page, len(raw_things))

    return raw_things


def _parse_uom(raw: dict) -> dict:
    return {
        "name": raw.get("name"),
        "symbol": raw.get("symbol"),
        "definition": raw.get("definition"),
    }


def _parse_observed_property(raw: dict) -> Optional[dict]:
    if not raw.get("@iot.id"):
        return None
    return {
        "id": raw["@iot.id"],
        "self_link": raw.get("@iot.selfLink", ""),
        "name": raw.get("name", ""),
        "description": raw.get("description"),
        "definition": raw.get("definition"),
        "properties": raw.get("properties") or None,
    }


def _parse_sensor(raw: dict) -> Optional[dict]:
    if not raw.get("@iot.id"):
        return None
    return {
        "id": raw["@iot.id"],
        "self_link": raw.get("@iot.selfLink", ""),
        "name": raw.get("name", ""),
        "description": raw.get("description"),
        "encoding_type": raw.get("encodingType", ""),
        "metadata": raw.get("metadata"),
        "properties": raw.get("properties") or None,
    }


def _parse_datastream(raw: dict, thing_id: int) -> Optional[dict]:
    ds_id = raw.get("@iot.id")
    if ds_id is None:
        return None
    uom_raw = raw.get("unitOfMeasurement")
    return {
        "id": ds_id,
        "self_link": raw.get("@iot.selfLink", ""),
        "name": raw.get("name", ""),
        "description": raw.get("description"),
        "phenomenon_time": raw.get("phenomenonTime"),
        "result_time": raw.get("resultTime"),
        "observed_area": raw.get("observedArea"),
        "observation_type": raw.get("observationType"),
        "unit_of_measurement": _parse_uom(uom_raw) if uom_raw else None,
        "properties": raw.get("properties") or None,
        "observed_property": _parse_observed_property(raw["ObservedProperty"])
            if raw.get("ObservedProperty") else None,
        "sensor": _parse_sensor(raw["Sensor"])
            if raw.get("Sensor") else None,
    }


def _parse_location(raw: dict) -> Optional[dict]:
    if not raw.get("@iot.id"):
        return None
    return {
        "id": raw["@iot.id"],
        "name": raw.get("name", ""),
        "geometry": raw.get("location"),
    }


def _parse_thing(raw: dict) -> Optional[HarvestedThing]:
    thing_id = raw.get("@iot.id")
    if thing_id is None:
        return None

    locations = [
        loc for r in raw.get("Locations", [])
        if (loc := _parse_location(r)) is not None
    ]
    datastreams = [
        ds for r in raw.get("Datastreams", [])
        if (ds := _parse_datastream(r, thing_id)) is not None
    ]

    return HarvestedThing(
        id=thing_id,
        self_link=raw.get("@iot.selfLink", ""),
        name=raw.get("name", ""),
        description=raw.get("description"),
        properties=raw.get("properties") or None,
        locations=locations,
        datastreams=datastreams,
    )


async def fetch_catalog(config: BenchmarkConfig) -> tuple[HarvestedCatalog, float]:
    """Fetch raw STA metadata and normalise into HarvestedCatalog."""
    timeout = aiohttp.ClientTimeout(total=config.timeout_seconds)
    expand = "Locations,Datastreams($expand=ObservedProperty,Sensor)"
    initial_url = (
        f"{config.sta_base_url}/Things"
        f"?$expand={expand}&$top={config.page_size}"
    )

    logger.info("Fetching from %s", config.sta_base_url)
    fetch_start = time.monotonic()

    async with aiohttp.ClientSession(timeout=timeout) as session:
        raw_things = await _paginate_things(session, initial_url)

    fetch_elapsed = time.monotonic() - fetch_start

    things = [t for r in raw_things if (t := _parse_thing(r)) is not None]
    total_ds = sum(len(t.datastreams) for t in things)
    logger.info(
        "Fetch complete: %d Things, %d Datastreams, fetch_time=%.3fs",
        len(things), total_ds, fetch_elapsed,
    )

    catalog = HarvestedCatalog(
        base_url=config.sta_base_url,
        conformance=[],
        things=things,
        harvested_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    return catalog, fetch_elapsed

