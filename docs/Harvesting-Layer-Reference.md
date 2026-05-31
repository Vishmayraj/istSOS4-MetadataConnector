# Harvesting Layer Reference

**Project:** istSOS Metadata Connector for Data Spaces and STAC
**Author:** Zala Vishmayraj
**Status:** Design, pre-implementation
**Scope:** `connector/harvester.py`, `connector/cache.py`, internal data models consumed by `stac_transformer.py` and `dcat_transformer.py`

---

## Design

This section is a self-contained overview of the harvesting layer. It is intended for quick review.

**What it does.** The harvester fetches metadata from a live istSOS4 SensorThings API instance, normalizes it into Python dataclasses and dicts, and stores the result in an in-memory TTL cache. The STAC and DCAT-AP transformers consume the cached representation and never touch the SensorThings API directly.

**What it does not do.** The harvester does not compute derived fields. Temporal extents, spatial bounding boxes, keyword sets, observable property deduplication, none of that happens here. The harvester stores what the API returns. All computation belongs in the transformers, which consume the same normalized data but derive different things from it. This boundary is deliberate: the STAC and DCAT-AP transformers need the same raw data but produce structurally different outputs. Computing shared derived fields in the harvester would mean making transformer-level decisions in the wrong layer, and would create hidden coupling between the transformers via harvester side effects.

**The single harvesting query.** One paginated request sequence collects everything both transformers need:

```
GET /Things?$expand=Locations,Datastreams($expand=ObservedProperty,Sensor)&$top={page_size}
```

I verified this against a local istSOS4 instance with dummy data. Nested expand works. One page sequence returns Thing identity, inline Locations with GeoJSON geometry, inline Datastreams with phenomenonTime, observedArea, unitOfMeasurement, and each Datastream's ObservedProperty and Sensor inline. The alternative was separate requests per entity type and per entity which makes O(1 + N + N + 4N + 4N) = O(10N) requests for N Things. The single expanded query makes ceil(N / page_size) requests.

**Pagination.** My original PRs #130 and #131 assumed `@iot.nextLink` should always be present (as null on the last page) and `@iot.count` should always be emitted. The claudio's correction: per OGC STA 1.1, `@iot.nextLink` is only required when a next page exists, and `@iot.count` is optional. Absent and null are both "last page." The harvester uses `.get("@iot.nextLink")` with a truthiness check throughout, outer Things pagination, inner `Datastreams@iot.nextLink`, inner `Locations@iot.nextLink`. `@iot.count` is not used anywhere in the pagination logic.

**Internal data model.** Two dataclasses: `HarvestedThing` and `HarvestedCatalog`. Everything nested inside a Thing, Locations, Datastreams, ObservedProperties, Sensors, UnitOfMeasurements, is stored as normalized dicts. The main normalization decisions: `@iot.id` keys become `id`, `@iot.selfLink` becomes `self_link`, `camelCase` API keys become `snake_case`, the Location `location` key (which holds the GeoJSON geometry) is renamed to `geometry` to avoid confusion with the entity itself, and all optional fields become `None` when null or absent rather than being absent from the dict.

**HarvestedCatalog(dataclass):**
```
base_url: str             # STA_BASE_URL, no trailing slash
conformance: list[str]    # from serverSettings.conformance on root response
things: list[HarvestedThing]
harvested_at: str         # ISO 8601 UTC, set when last page received
thing_count: int          # always equals len(things)
```

**HarvestedThing(dataclass):**
```
id: int
self_link: str
name: str
description: str | None
properties: dict | None
locations: list[dict]     # always a list, empty if no Locations
datastreams: list[dict]   # always a list, empty if no Datastreams
```

**Location dict:**
```
id: int
self_link: str
name: str
description: str | None
properties: dict | None

encoding_type: str
geometry: dict | None     # GeoJSON dict, renamed from API key "location"
```

**Datastream dict:**
```
id: int
self_link: str
name: str
description: str | None
properties: dict | None

phenomenon_time: str | None       # raw interval string "start/end"
result_time: str | None
observed_area: dict | None        # raw GeoJSON Polygon dict
observation_type: str | None
unit_of_measurement: dict | None
observed_property: dict | None
sensor: dict | None
```

**UnitOfMeasurement dict:**
```
name: str | None
symbol: str | None
definition: str | None
```

**ObservedProperty dict:**
```
id: int
self_link: str
name: str
description: str | None
properties: dict | None

definition: str | None
```

**Sensor dict:**
```
id: int
self_link: str
name: str
description: str | None
properties: dict | None

encoding_type: str
metadata: str | None
```

**Thing field mapping** (`GET {STA_BASE_URL}/Things?$expand=Locations,Datastreams(...)`)

| API JSON key | Internal field | Type | Notes |
|---|---|---|---|
| `@iot.id` | `id` | `int` | Skip Thing and warn if missing |
| `@iot.selfLink` | `self_link` | `str` | Warn if missing, use `""` |
| `name` | `name` | `str` | Warn if missing, use `""` |
| `description` | `description` | `str\|None` | `None` if null or absent |
| `properties` | `properties` | `dict\|None` | `None` if null or absent |
| `Locations` | `locations` | `list[dict]` | Section 6.2. Empty list if absent |
| `Datastreams` | `datastreams` | `list[dict]` | Section 6.3. Follow `Datastreams@iot.nextLink`. Empty list if absent |

**Location field mapping** (`Thing.Locations[]` in expanded response)

| API JSON key | Dict key | Type | Notes |
|---|---|---|---|
| `@iot.id` | `id` | `int` | Skip Location and warn if missing |
| `@iot.selfLink` | `self_link` | `str` | Warn if missing, use `""` |
| `name` | `name` | `str` | Warn if missing, use `""` |
| `description` | `description` | `str\|None` | `None` if null or absent |
| `encodingType` | `encoding_type` | `str` | Default `"application/geo+json"` if missing |
| `location` | `geometry` | `dict\|None` | GeoJSON geometry dict. Key renamed from `location` to `geometry`. `None` if null or absent |
| `properties` | `properties` | `dict\|None` | `None` if null or absent |

**Datastream field mapping** (`Thing.Datastreams[]` in expanded response)

| API JSON key | Dict key | Type | Notes |
|---|---|---|---|
| `@iot.id` | `id` | `int` | Skip Datastream and warn if missing |
| `@iot.selfLink` | `self_link` | `str` | Warn if missing, use `""` |
| `name` | `name` | `str` | Warn if missing, use `""` |
| `description` | `description` | `str\|None` | `None` if null or absent |
| `phenomenonTime` | `phenomenon_time` | `str\|None` | Raw interval string. `None` if null or absent |
| `resultTime` | `result_time` | `str\|None` | `None` if null or absent |
| `observedArea` | `observed_area` | `dict\|None` | GeoJSON Polygon dict. `None` if null or absent |
| `observationType` | `observation_type` | `str\|None` | `None` if null or absent |
| `unitOfMeasurement` | `unit_of_measurement` | `dict\|None` | Parsed per Section 6.6. `None` if absent |
| `properties` | `properties` | `dict\|None` | `None` if null or absent |
| `ObservedProperty` | `observed_property` | `dict\|None` | Parsed per Section 6.4. `None` if absent or missing `@iot.id`. Warn if `None` |
| `Sensor` | `sensor` | `dict\|None` | Parsed per Section 6.5. `None` if absent or missing `@iot.id`. Warn if `None` |

**ObservedProperty field mapping** (`Datastream.ObservedProperty` in expanded response)

| API JSON key | Dict key | Type | Notes |
|---|---|---|---|
| `@iot.id` | `id` | `int` | Set parent `observed_property` to `None` and warn if missing |
| `@iot.selfLink` | `self_link` | `str` | Warn if missing, use `""` |
| `name` | `name` | `str` | Warn if missing, use `""` |
| `description` | `description` | `str\|None` | `None` if null or absent |
| `definition` | `definition` | `str\|None` | In dummy data this is `"{}"`, a string not a dict. Stored as-is |
| `properties` | `properties` | `dict\|None` | `None` if null or absent |

**Sensor field mapping** (`Datastream.Sensor` in expanded response)

| API JSON key | Dict key | Type | Notes |
|---|---|---|---|
| `@iot.id` | `id` | `int` | Set parent `sensor` to `None` and warn if missing |
| `@iot.selfLink` | `self_link` | `str` | Warn if missing, use `""` |
| `name` | `name` | `str` | Warn if missing, use `""` |
| `description` | `description` | `str\|None` | `None` if null or absent |
| `encodingType` | `encoding_type` | `str` | Default `""` if missing |
| `metadata` | `metadata` | `str\|None` | Primary field for DCAT-AP publisher metadata. `None` if null or absent |
| `properties` | `properties` | `dict\|None` | `None` if null or absent |

**UnitOfMeasurement field mapping** (`Datastream.unitOfMeasurement` in expanded response)

| API JSON key | Dict key | Type | Notes |
|---|---|---|---|
| `name` | `name` | `str\|None` | `None` if missing |
| `symbol` | `symbol` | `str\|None` | `None` if missing |
| `definition` | `definition` | `str\|None` | Empty string in dummy data. Stored as-is |

**Error handling summary:**

| Condition | Behavior |
|---|---|
| Network error / 5xx | Exponential backoff (1s, 2s, 4s), max 3 retries. Raises `HarvesterNetworkError` or `HarvesterServerError` if exhausted. Cache not updated |
| 4xx response | Not retried. Raises `HarvesterConfigurationError` immediately |
| Non-JSON body or missing `value` key | Raises `HarvesterParseError` immediately, no retry |
| Thing missing `@iot.id` | Logged as WARNING, Thing skipped, harvest continues |
| Entity missing `name` or `self_link` | Logged as WARNING, default `""` used, entity kept |
| Nested entity missing `@iot.id` | Logged as WARNING, parent dict key set to `None` |
| Harvest fails mid-page | Previous cache entry preserved. Partial catalog never committed |

**Cache summary:**

| Parameter | Value |
|---|---|
| Type | In-memory TTL cache, `connector/cache.py` |
| Storage | One `HarvestedCatalog` object per configured deployment |
| Cache key | `catalog:{STA_BASE_URL}` |
| Invalidation | TTL only. Default 300s, configurable |
| Write behavior | Atomic replacement on successful harvest. Never partially written |
| Read behavior | Transformers read full catalog by reference. Must not mutate |

**Configuration variables:**

| Variable | Type | Default | Description |
|---|---|---|---|
| `STA_BASE_URL` | `str` | required | Base URL of the SensorThings API. No trailing slash |
| `HARVESTER_PAGE_SIZE` | `int` | `100` | `$top` value per request |
| `HARVESTER_MAX_RETRIES` | `int` | `3` | Retry attempts per request |
| `HARVESTER_TIMEOUT_SECONDS` | `float` | `30.0` | Per-request HTTP timeout in seconds |
| `CACHE_TTL_SECONDS` | `int` | `300` | Cache TTL in seconds |

**Public interface:**
```python
async def harvest(config: Settings) -> HarvestedCatalog: ...
async def get_catalog(config: Settings, cache: Cache) -> HarvestedCatalog: ...
```
`harvest()` fetches and normalizes, never touches cache. `get_catalog()` is what the transformer layer calls, returns cached if valid, otherwise triggers harvest and stores result.

**Transformer contract (what downstream code can rely on):**

1. `HarvestedCatalog.things` is always a list. Never `None`. May be empty.
2. `HarvestedThing.locations` is always a list. Never `None`. May be empty.
3. `HarvestedThing.datastreams` is always a list. Never `None`. May be empty.
4. Every `HarvestedThing.id` is unique within a `HarvestedCatalog`.
5. Every Datastream dict `id` is globally unique across all Things in the catalog.
6. `geometry` in a Location dict, if not `None`, is a dict with at minimum a `"type"` key. Not validated as well-formed GeoJSON.
7. `phenomenon_time` in a Datastream dict, if not `None`, is a string in `"start/end"` format where start and end are ISO 8601 instants. Not parsed.
8. `observed_area` in a Datastream dict, if not `None`, is a dict with at minimum a `"type"` key.
9. `name` and `self_link` on `HarvestedThing` are never `None`. They default to `""` with a warning. Same applies inside Location, Datastream, ObservedProperty, and Sensor dicts.
10. The cache is never partially written.
11. `HarvestedCatalog.base_url` has no trailing slash.
12. `HarvestedCatalog.harvested_at` is a valid ISO 8601 UTC string.
13. `HarvestedCatalog` and its nested structures must not be mutated by the transformer.
