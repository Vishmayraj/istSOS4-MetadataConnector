"""
Connector configuration layer.

All settings are loaded from environment variables or a .env file in the
project root. The Settings class is the single source of truth for every
configurable value. Import get_settings() everywhere — never read os.environ
directly.

Variable names match the spec in Harvesting-Layer-Reference.md §11 exactly
so that .env keys are self-documenting without a translation layer.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Connector configuration loaded from environment variables or .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # SensorThings API connection
    STA_BASE_URL: str = Field(
        default="http://localhost:8018/istsos4/v1.1",
        description=(
            "Base URL of the OGC SensorThings API v1.1 endpoint. "
            "No trailing slash. e.g. http://localhost:8018/istsos4/v1.1"
        ),
    )

    # Harvester tuning
    HARVESTER_PAGE_SIZE: int = Field(
        default=100,
        description="$top value per paginated Things request.",
    )
    HARVESTER_MAX_RETRIES: int = Field(
        default=3,
        description="Retry attempts per request for network errors and 5xx responses.",
    )
    HARVESTER_TIMEOUT_SECONDS: float = Field(
        default=30.0,
        description="Per-request HTTP timeout in seconds.",
    )

    # Cache
    CACHE_TTL_SECONDS: int = Field(
        default=300,
        description="Cache TTL in seconds. Default 300 (5 minutes).",
    )

    # STAC output
    STAC_ROOT_HREF: str = Field(
        default="http://localhost:8020/stac",
        description="Public base URL of the STAC endpoint, used for building self/root/collection links.",
    )
