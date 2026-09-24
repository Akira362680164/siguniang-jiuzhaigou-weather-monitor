#!/usr/bin/env python3
"""Open-Meteo-only weather evidence pipeline for the Siguniang-Jiuzhaigou autumn monitor."""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import gzip
import hashlib
import json
import math
import os
import re
import shutil
import ssl
import sys
import time
from pathlib import Path
from statistics import mean, median
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

try:
    import certifi
except ImportError:  # pragma: no cover - CI installs requirements.txt
    certifi = None


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "points.json"
NAMESPACE_NAME = "siguniang_jiuzhaigou"
LATEST_DIR = ROOT / "data" / "latest"
ARCHIVE_DIR = ROOT / "data" / "archive"
HISTORY_CACHE_DIR = ROOT / "data" / "cache" / "history"
WEATHER_EVENTS_CACHE_DIR = ROOT / "data" / "cache" / "weather_events"
TIMEZONE_NAME = "Asia/Shanghai"
LOCAL_TZ = ZoneInfo(TIMEZONE_NAME)
UTC = dt.timezone.utc
SCHEMA_VERSION = "1.4.0"
LEGACY_SCHEMA_VERSION = "1.0.0"
PREVIOUS_SCHEMA_VERSION = "1.1.0"
PRIOR_SCHEMA_VERSION = "1.2.0"
PRIOR_MINOR_SCHEMA_VERSION = "1.3.0"
COMPATIBLE_SCHEMA_VERSIONS = {
    LEGACY_SCHEMA_VERSION,
    PREVIOUS_SCHEMA_VERSION,
    PRIOR_SCHEMA_VERSION,
    PRIOR_MINOR_SCHEMA_VERSION,
    SCHEMA_VERSION,
}
RAW_RETENTION_DAYS = 14
HRES_GRID_QA_LIMIT_KM = 14.0
HISTORY_GRID_QA_LIMIT_KM = 13.5
LONG_RANGE_GRID_QA_LIMIT_KM = 35.0
SITE_GRID_QA_CONFIG_KEY = "grid_qa"
SITE_HRES_GRID_QA_LIMIT_KEY = "hres_limit_km"

OPEN_METEO_ENDPOINTS = {
    "hres": "https://api.open-meteo.com/v1/ecmwf",
    "history": "https://archive-api.open-meteo.com/v1/archive",
    "gfs": "https://api.open-meteo.com/v1/gfs",
    "ensemble": "https://ensemble-api.open-meteo.com/v1/ensemble",
    "single_runs": "https://single-runs-api.open-meteo.com/v1/forecast",
}
ALLOWED_HOSTS = {
    "api.open-meteo.com",
    "archive-api.open-meteo.com",
    "ensemble-api.open-meteo.com",
    "single-runs-api.open-meteo.com",
}

# ---------------------------------------------------------------------------
# Three-year historical comparison (archive API)
# ---------------------------------------------------------------------------
# The comparison module answers "what did the target window look like in the
# previous three years".  It queries the archive endpoint with daily aggregates
# for the day-level view and hourly precipitation / cloud for the day-part
# split, so it never mixes analysis values into the forecast modules.
HISTORICAL_DAILY_VARIABLES = [
    "temperature_2m_mean",
    "temperature_2m_min",
    "temperature_2m_max",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "precipitation_hours",
]
HISTORICAL_HOURLY_VARIABLES = ["precipitation", "rain", "snowfall", "cloud_cover"]
HISTORICAL_DAYPARTS = (
    ("night_00_06", 0, 6),
    ("morning_06_12", 6, 12),
    ("afternoon_12_18", 12, 18),
    ("evening_18_24", 18, 24),
)

# ---------------------------------------------------------------------------
# Unified weather variable system (schema 1.4.0)
# ---------------------------------------------------------------------------
# Every forecast model requests and reports the same variable vocabulary.  The
# four model families stay independent: ECMWF deterministic (HRES), ECMWF
# ensemble, GFS deterministic and the GEFS ensemble.  Values are never averaged
# across models and a missing variable is reported as unavailable instead of
# being filled from another model.
UNIFIED_CORE_VARIABLES = (
    "temperature_2m",
    "dew_point_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "snowfall",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
)
UNIFIED_SOLAR_VARIABLES = ("sunshine_duration", "shortwave_radiation")
UNIFIED_WEATHER_VARIABLES = (*UNIFIED_CORE_VARIABLES, "sunshine_duration")
# Cloud layers must always come from the API.  Never derive mid/high cloud by
# subtracting low cloud from the total: the layers overlap and are not additive.
CLOUD_LAYER_VARIABLES = (
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
)
# A published viewing signal is read from GEFS when GEFS supplies it and from
# the independent ECMWF ensemble otherwise.  These pairs let a summary name the
# source of every signal next to the signal itself, so a single-ensemble value
# can never read as an EC + GEFS merge.
VIEWING_SIGNAL_SOURCE_VARIABLES = (
    ("total_cloud", "cloud_cover_gt_70pct", "cloud_cover"),
    ("precip", "precipitation_gt_0_5mm", "precipitation"),
    ("snow", "snowfall_gt_0_5cm", "snowfall"),
    ("wind", "gust_gt_50kmh", "wind_gusts_10m"),
    ("low_cloud", "cloud_cover_low_gt_50pct", "cloud_cover_low"),
    ("mid_cloud", "cloud_cover_mid_gt_50pct", "cloud_cover_mid"),
    ("high_cloud", "cloud_cover_high_gt_50pct", "cloud_cover_high"),
)
# Sustained / mean wind speed is a different quantity from the gust.  Keep the
# two names separate so a summary can never describe a gust as sustained wind.
SUSTAINED_WIND_VARIABLES = ("wind_speed_10m",)
GUST_VARIABLES = ("wind_gusts_10m",)
GUST_THRESHOLD_LEVELS_KMH = (30.0, 40.0, 50.0, 60.0)
CORE_REGION_VARIABLE_REQUIRED = (
    "temperature_2m",
    "precipitation",
    "snowfall",
    "cloud_cover",
    "wind_gusts_10m",
)

HRES_VARIABLES = [*UNIFIED_WEATHER_VARIABLES]
HRES_REQUIRED_VARIABLES = [value for value in CORE_REGION_VARIABLE_REQUIRED]
HRES_OPTIONAL_VARIABLES = [
    value for value in HRES_VARIABLES if value not in HRES_REQUIRED_VARIABLES
]
HRES_FALLBACK_SOLAR = "shortwave_radiation"
GFS_VARIABLES = [*UNIFIED_WEATHER_VARIABLES]
GFS_REQUIRED_VARIABLES = [value for value in CORE_REGION_VARIABLE_REQUIRED]
GFS_OPTIONAL_VARIABLES = [
    value for value in GFS_VARIABLES if value not in GFS_REQUIRED_VARIABLES
]
# The historical archive reader intentionally keeps the pre-1.4 variable set.
# Widening it would change the historical request, re-trim cache rows and force
# a full history re-download for no analytical gain.
HISTORY_VARIABLES = [
    "temperature_2m",
    "precipitation",
    "snowfall",
    "cloud_cover",
    "cloud_cover_low",
    "sunshine_duration",
    "wind_speed_10m",
    "wind_gusts_10m",
]
HISTORY_REQUIRED_VARIABLES = [
    value for value in HISTORY_VARIABLES if value != "sunshine_duration"
]
# ECMWF single-run reproducibility comparison is not one of the four unified
# model families; keep its request frozen so its run-to-run delta stays
# comparable with the already published history.
SINGLE_RUN_VARIABLES = [
    "temperature_2m",
    "precipitation",
    "snowfall",
    "cloud_cover",
    "cloud_cover_low",
    "sunshine_duration",
    "wind_speed_10m",
    "wind_gusts_10m",
]
SINGLE_RUN_REQUIRED_VARIABLES = [
    value for value in SINGLE_RUN_VARIABLES if value != "sunshine_duration"
]
# ECMWF IFS does not publish every cycle with the same forecast length: the
# 00Z/12Z runs carry the full horizon while the 06Z/18Z runs are short runs.
# A run-to-run drift comparison therefore only requires the long cycles; a
# missing short cycle is a model property, not a data failure.
SINGLE_RUN_LONG_CYCLE_HOURS = (0, 12)
SINGLE_RUN_SHORT_CYCLE_HOURS = (6, 18)
SINGLE_RUN_MIN_REQUIRED_RUNS = 2
ENSEMBLE_VARIABLES = [*UNIFIED_WEATHER_VARIABLES]
EC_ENSEMBLE_REQUIRED_VARIABLES = [value for value in CORE_REGION_VARIABLE_REQUIRED]
EC_ENSEMBLE_OPTIONAL_VARIABLES = [
    value for value in ENSEMBLE_VARIABLES if value not in EC_ENSEMBLE_REQUIRED_VARIABLES
]
EC_ENSEMBLE_TOTAL_MEMBERS = 51
# Wind direction is a circular quantity: it is stored and validated hourly but
# never reduced with an arithmetic mean into an ensemble distribution.
ENSEMBLE_DISTRIBUTION_VARIABLES = tuple(
    value for value in ENSEMBLE_VARIABLES if value != "wind_direction_10m"
)
ECMWF_ENSEMBLE_FORECAST_DAYS = 15
LONG_RANGE_MODEL_ID = "ncep_gefs05"
LONG_RANGE_MODEL = "GFS Ensemble 0.5°"
LONG_RANGE_ENSEMBLE_MEMBERS = 31
LONG_RANGE_REQUESTED_FORECAST_DAYS = 35
LONG_RANGE_LEAD_START = 16
LONG_RANGE_LEAD_END = 35
# `ncep_gefs05` documents about 35 forecast days and only the freshest long run
# extends to the very last published block.  The daily run time happens before
# that run is disseminated, so the trailing block D34_D35 is best effort while
# the last block needed for the declared background signal is D31_D33.
LONG_RANGE_REQUIRED_LEAD_END = 33
LONG_RANGE_VARIABLES = ["temperature_2m", "precipitation", "snowfall", "wind_gusts_10m"]
LONG_RANGE_ENDPOINT_DOC = "https://open-meteo.com/en/docs/ensemble-api"
LONG_RANGE_MODEL_REGISTRY_DOC = "https://github.com/open-meteo/open-meteo/blob/main/openapi/ensemble.yml"
GEFS_NEAR_MODEL_ID = "ncep_gefs025"
GEFS_NEAR_MODEL = "GFS Ensemble 0.25°"
GEFS_NEAR_RESOLUTION = "0.25° (~25 km)"
GEFS_NEAR_FORECAST_DAYS = 10
GEFS_LONG_MODEL_ID = "ncep_gefs05"
GEFS_LONG_MODEL = "GFS Ensemble 0.5°"
GEFS_LONG_RESOLUTION = "0.5° (~50 km)"
GEFS_LONG_FORECAST_DAYS = 35
GEFS_ENSEMBLE_MEMBERS = 31
GEFS_GRID_QA_LIMITS_KM = {"near_range": 25.0, "long_range": 40.0}
GEFS_CACHE_DIR = ROOT / "data" / "cache" / "gefs"
GEFS_TRAVEL_CUTOFF_DATE = dt.date(2026, 11, 1)
GEFS_CORE_VARIABLES = [
    *UNIFIED_WEATHER_VARIABLES,
]
# The Open-Meteo GEFS response currently returns cloud_cover_low/mid/high as
# null arrays for western Sichuan.  Keep the required/optional boundary explicit so
# that an unavailable capability does not invalidate otherwise usable member
# distributions.  The requested variables remain in GEFS_CORE_VARIABLES for
# one auditable API request.
GEFS_REQUIRED_VARIABLES = (
    "temperature_2m",
    "precipitation",
    "snowfall",
    "cloud_cover",
    "wind_gusts_10m",
)
GEFS_OPTIONAL_SOLAR_VARIABLES = ("shortwave_radiation", "sunshine_duration")
GEFS_OPTIONAL_VARIABLES = (
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "relative_humidity_2m",
    "dew_point_2m",
    "rain",
    "wind_speed_10m",
    "wind_direction_10m",
    *GEFS_OPTIONAL_SOLAR_VARIABLES,
)
# Variable availability vocabulary used by status.json and the module artifacts.
VARIABLE_STATUS_OK = "OK"
VARIABLE_STATUS_PARTIAL = "PARTIAL"
VARIABLE_STATUS_REQUIRED_UNAVAILABLE = "REQUIRED_UNAVAILABLE"
VARIABLE_STATUS_OPTIONAL_UNAVAILABLE = "OPTIONAL_UNAVAILABLE"
VARIABLE_STATUS_MISSING = "MISSING"
VARIABLE_STATUS_LENGTH_MISMATCH = "ARRAY_LENGTH_MISMATCH"
VARIABLE_STATUS_NULL_ARRAY = "NULL_ARRAY"
VARIABLE_STATUS_PARTIAL_NULL = "PARTIAL_NULL"
UNAVAILABLE_STATUSES = frozenset({
    VARIABLE_STATUS_REQUIRED_UNAVAILABLE,
    VARIABLE_STATUS_OPTIONAL_UNAVAILABLE,
    VARIABLE_STATUS_MISSING,
    VARIABLE_STATUS_LENGTH_MISMATCH,
    VARIABLE_STATUS_NULL_ARRAY,
})
GEFS_PHASE_RULE_VERSION = "gefs_event_phase_v1"
GEFS_PHASE_THRESHOLDS = {
    "cloud_cover_pct": 70.0,
    "cloud_cover_low_pct": 50.0,
    "precipitation_mm": 0.5,
    "snowfall_cm": 0.1,
    "cold_daily_mean_drop_c": 3.0,
    "cold_daily_tmin_c": 0.0,
    "minimum_event_duration_hours": 3,
    "maximum_event_gap_hours": 6,
    "high_phase_support": 0.7,
    "medium_phase_support": 0.5,
    "high_phase_spread_hours": 12,
    "medium_phase_spread_hours": 24,
}
GEFS_WINDOW_DEFINITIONS = {
    "MORNING": (8, 12),
    "AFTERNOON": (12, 18),
    "NIGHT": (18, 8),
}
TARGET_WINDOW_NAMES = ("MORNING", "AFTERNOON", "NIGHT")
# Hemi morning-fog inputs.  These are raw indicators only; the pipeline never
# emits a fabricated "fog probability".
FOG_PRE_DAWN_START_HOUR = 4
FOG_PRE_DAWN_END_HOUR = 8
FOG_NIGHT_HOURS = 12
FOG_DAY_HOURS = 24
FOG_RADIATIVE_COOLING_SPREAD_C = 3.0
FOG_WIND_CALM_KMH = 8.0
FOG_WIND_BREAKUP_KMH = 18.0
MODEL_CONSISTENCY_PAIRS = (
    "ec_hres_vs_ec_ensemble",
    "gfs_deterministic_vs_gefs",
    "ec_hres_vs_gfs_deterministic",
    "ec_ensemble_vs_gefs",
)
CORE_REGION_IDS = ("siguniang", "lixiaolu", "jiuzhaigou")
HISTORY_MODEL = "ECMWF IFS 9 km historical weather / analysis"
HISTORY_MODEL_PARAMETER = "ecmwf_ifs"
THRESHOLDS_C = (15.0, 10.0, 5.0, 2.0, 0.0)
DEFAULT_HISTORY_YEARS = (2025, 2026)
HISTORY_FORWARD_YEARS = (2023, 2024, 2025)
HISTORY_FORWARD_CUTOFF_MONTH_DAY = "11-01"
HISTORY_FORWARD_WINDOW_KEYS = ("d0_7", "d8_15", "d16_to_11_01")
# A rolling window can run past the hard cutoff once the anchor date advances.
# Such a window is structurally empty, not a data failure, so it is marked
# NOT_APPLICABLE and excluded from every OK/INVALID judgement.
HISTORY_FORWARD_WINDOW_NOT_APPLICABLE = "NOT_APPLICABLE"
HISTORY_FORWARD_WINDOW_CLOSED_REASON = "HISTORY_FORWARD_WINDOW_CLOSED"
WEATHER_EVENTS_CUTOFF = dt.date(2026, 11, 1)
WEATHER_EVENTS_CACHE_SCHEMA_VERSION = "1.0.0"
WEATHER_EVENT_RULE_VERSION = "weather_events_v1"
COOLING_EPISODE_RULE_VERSION = "cooling_episode_v1"
MECHANICAL_LEAF_STRESS_RULE_VERSION = "mechanical_leaf_stress_v1"
COOLING_BASELINE_DAYS = 3
COOLING_MIN_MEAN_DROP_C = 3.0
COOLING_MIN_NIGHT_DROP_C = 2.0
COOLING_ACTIVE_DAY_TOLERANCE_C = 1.0
COOLING_RECOVERY_TOLERANCE_C = 0.5
COOLING_MAX_RECOVERY_DAYS = 3
COOLING_MIN_EPISODE_SEPARATION_DAYS = 2

PRECISION_POLICIES = {
    "hres": [
        {"from_lead_hours": 0, "to_lead_hours_exclusive": 90, "precision_class": "native_hourly"},
        {"from_lead_hours": 90, "to_lead_hours_exclusive": 144, "precision_class": "coarse_3h_interpolated"},
        {"from_lead_hours": 144, "to_lead_hours_exclusive": None, "precision_class": "trend_only_6h_plus"},
    ],
    "gfs": [
        {"from_lead_hours": 0, "to_lead_hours_exclusive": 120, "precision_class": "native_hourly"},
        {"from_lead_hours": 120, "to_lead_hours_exclusive": None, "precision_class": "coarse_3h_interpolated"},
    ],
    "single_runs": [
        {"from_lead_hours": 0, "to_lead_hours_exclusive": 90, "precision_class": "native_hourly"},
        {"from_lead_hours": 90, "to_lead_hours_exclusive": 144, "precision_class": "coarse_3h_interpolated"},
        {"from_lead_hours": 144, "to_lead_hours_exclusive": None, "precision_class": "trend_only_6h_plus"},
    ],
}


class OpenMeteoError(RuntimeError):
    """A request failed against an allow-listed Open-Meteo endpoint."""

    def __init__(self, reason: str, *, status_code: int | None = None, body: str = "") -> None:
        super().__init__(reason)
        self.reason = reason
        self.status_code = status_code
        self.body = body


def log(message: str) -> None:
    print(message, flush=True)


def now_from_input(value: str | None = None) -> dt.datetime:
    raw = value or os.environ.get("SIGUNIANG_JIUZHAIGOU_MONITOR_NOW")
    if raw:
        parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return dt.datetime.now(UTC)


def iso_utc(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def iso_local(value: dt.datetime) -> str:
    return value.astimezone(LOCAL_TZ).isoformat(timespec="minutes")


def date_string(value: dt.date) -> str:
    return value.isoformat()


def parse_local_api_time(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value).replace(tzinfo=LOCAL_TZ)


def round_or_none(value: object, digits: int = 3) -> object:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return round(float(value), digits)
    return value


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
    temp_path.replace(path)


def write_compact_json(path: Path, value: object) -> None:
    """Write a machine-facing artifact without pretty-print whitespace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False)
        handle.write("\n")
    temp_path.replace(path)


def write_gzip_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    with gzip.open(temp_path, "wt", encoding="utf-8", compresslevel=9) as handle:
        json.dump(value, handle, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    temp_path.replace(path)


def validate_target_groups_config(config: dict) -> None:
    """Validate the itinerary target groups that drive target_summary.json."""
    target_dates: set[str] = set()
    for group_id, group in (config.get("target_groups") or {}).items():
        if not group.get("dates") or not group.get("point_ids"):
            raise ValueError(f"target group {group_id} needs dates and point_ids")
        for raw_date in group["dates"]:
            dt.date.fromisoformat(raw_date)
            target_dates.add(raw_date)
        missing = set(group["point_ids"]) - set(config.get("points", {}))
        if missing:
            raise ValueError(f"target group {group_id} references missing points: {sorted(missing)}")
    if not target_dates:
        raise ValueError("target_groups must contain at least one target date")


def validate_historical_comparison_config(config: dict) -> None:
    """Validate the three-year same-calendar-window comparison settings."""
    historical = config.get("historical_comparison")
    if not isinstance(historical, dict):
        raise ValueError("historical_comparison must be configured")
    years = historical.get("years")
    window_days = historical.get("window_days_each_side")
    if (
        not isinstance(years, list)
        or len(years) != 3
        or any(isinstance(year, bool) or not isinstance(year, int) for year in years)
        or len(set(years)) != len(years)
    ):
        raise ValueError("historical_comparison.years must contain three unique integer years")
    if isinstance(window_days, bool) or not isinstance(window_days, int) or window_days < 0 or window_days > 14:
        raise ValueError("historical_comparison.window_days_each_side must be an integer from 0 to 14")


def load_config(path: Path = CONFIG_PATH) -> dict:
    with path.open(encoding="utf-8") as handle:
        config = json.load(handle)
    if config.get("timezone") != TIMEZONE_NAME:
        raise ValueError(f"config timezone must be {TIMEZONE_NAME}")
    if config.get("schema_version") not in {LEGACY_SCHEMA_VERSION, PREVIOUS_SCHEMA_VERSION, SCHEMA_VERSION}:
        raise ValueError("unsupported points schema version")
    if config.get("namespace") != NAMESPACE_NAME:
        raise ValueError("unexpected config namespace")
    if not isinstance(config.get("points"), dict) or not config["points"]:
        raise ValueError("points must be a non-empty object")
    validate_siguniang_subregion_config(config)
    validate_jiuzhaigou_subregion_config(config)
    validate_target_groups_config(config)
    validate_historical_comparison_config(config)
    return config


def history_years_for_config(config: dict) -> tuple[int, ...]:
    """Return the configured historical years in stable ascending order."""
    raw_years = config.get("history_years", DEFAULT_HISTORY_YEARS)
    if not isinstance(raw_years, (list, tuple)) or not raw_years:
        raise ValueError("history_years must be a non-empty list")
    try:
        years = tuple(int(year) for year in raw_years)
    except (TypeError, ValueError) as error:
        raise ValueError("history_years must contain integers") from error
    if any(year < 1900 or year > 2100 for year in years):
        raise ValueError("history_years contains an out-of-range year")
    if years != tuple(sorted(set(years))):
        raise ValueError("history_years must be unique and ascending")
    return years


def valid_coordinate(latitude: object, longitude: object) -> bool:
    return (
        isinstance(latitude, (int, float))
        and not isinstance(latitude, bool)
        and -90 <= float(latitude) <= 90
        and isinstance(longitude, (int, float))
        and not isinstance(longitude, bool)
        and -180 <= float(longitude) <= 180
    )


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return radius_km * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def active_points(config: dict) -> dict[str, dict]:
    """Return only VERIFIED points; this is the main-chain trust boundary."""
    points = {}
    for point_id, point in config["points"].items():
        if point.get("status") != "VERIFIED":
            continue
        if not valid_coordinate(point.get("latitude"), point.get("longitude")):
            raise ValueError(f"invalid VERIFIED coordinate: {point_id}")
        points[point_id] = {"id": point_id, **point}
    return points


def core_region_ids(config: dict) -> tuple[str, ...]:
    """Return configured regions with a core point, preserving config order."""
    return tuple(
        region_id
        for region_id, region in config.get("regions", {}).items()
        if region.get("core_point_id")
    )


def point_forecast_end_date(point: dict) -> dt.date | None:
    value = point.get("forecast_end_date")
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value))
    except ValueError as error:
        raise ValueError(f"invalid forecast_end_date for {point.get('id')}: {value}") from error


def excluded_points(config: dict) -> dict[str, dict]:
    excluded = {}
    for point_id, point in config["points"].items():
        if point.get("status") != "VERIFIED":
            excluded[point_id] = {
                "name": point.get("name"),
                "region": point.get("region"),
                "status": point.get("status"),
                "usable_for_main_chain": False,
                "reason": point.get("reason") or "PROVISIONAL_POINT_EXCLUDED",
            }
    for slot_id, slot in config.get("route_slots", {}).items():
        excluded[slot_id] = {
            "name": slot.get("name"),
            "status": slot.get("status"),
            "usable_for_main_chain": False,
            "reason": slot.get("reason") or "ROUTE_NOT_VERIFIED",
        }
    return excluded


class ApiClient:
    def __init__(self, retries: int = 3, timeout_seconds: int = 45) -> None:
        self.retries = retries
        self.timeout_seconds = timeout_seconds
        self.ssl_context = ssl.create_default_context(cafile=certifi.where() if certifi else None)

    def get_json(
        self,
        endpoint: str,
        params: dict[str, object],
        label: str,
        *,
        allow_array: bool = False,
    ) -> tuple[dict | list, str]:
        host = endpoint.split("/", 3)[2]
        if host not in ALLOWED_HOSTS:
            raise ValueError(f"blocked non-Open-Meteo host: {host}")
        query = urlencode({key: str(value) for key, value in params.items()})
        url = f"{endpoint}?{query}"
        last_error: OpenMeteoError | None = None
        for attempt in range(1, self.retries + 1):
            try:
                request = Request(url, headers={"Accept": "application/json", "User-Agent": "siguniang-jiuzhaigou-weather-monitor/1.1"})
                with urlopen(request, timeout=self.timeout_seconds, context=self.ssl_context) as response:
                    body = response.read().decode("utf-8")
                payload = json.loads(body)
                # Multi-coordinate requests return one object per location rather
                # than a single object; only the historical comparison asks for
                # that shape, so it has to be opted into explicitly.
                if allow_array and isinstance(payload, list):
                    return payload, url
                if not isinstance(payload, dict):
                    raise OpenMeteoError("OPEN_METEO_INVALID_JSON_OBJECT", body=body[:500])
                if payload.get("error") is True:
                    raise OpenMeteoError(str(payload.get("reason") or "OPEN_METEO_API_ERROR"), body=body[:1000])
                return payload, url
            except HTTPError as error:
                body = error.read().decode("utf-8", errors="replace")
                try:
                    parsed = json.loads(body)
                    reason = str(parsed.get("reason") or parsed.get("message") or f"HTTP_{error.code}")
                except json.JSONDecodeError:
                    reason = f"HTTP_{error.code}"
                last_error = OpenMeteoError(reason, status_code=error.code, body=body[:1000])
            except (URLError, TimeoutError, OSError, json.JSONDecodeError, OpenMeteoError) as error:
                if isinstance(error, OpenMeteoError):
                    last_error = error
                else:
                    last_error = OpenMeteoError(f"{type(error).__name__}: {error}")
            if attempt < self.retries:
                delay = 2 ** (attempt - 1)
                log(f"[{label}] REQUEST RETRY {attempt}/{self.retries - 1} IN {delay}s: {last_error.reason}")
                time.sleep(delay)
        assert last_error is not None
        raise last_error


def is_variable_error(error: OpenMeteoError) -> bool:
    text = f"{error.reason} {error.body}".lower()
    return any(
        token in text
        for token in (
            "invalid variable",
            "unknown variable",
            "not available",
            "cannot find variable",
            "invalid value for",
        )
    )


def precision_class_for_lead(lead_hours: float, module: str) -> str:
    for item in PRECISION_POLICIES[module]:
        end = item["to_lead_hours_exclusive"]
        if lead_hours >= item["from_lead_hours"] and (end is None or lead_hours < end):
            return item["precision_class"]
    return "undetermined"


def add_precision_classes(hourly: dict, module: str) -> dict:
    output = dict(hourly)
    times = hourly.get("time") or []
    if not times:
        output["precision_class"] = []
        return output
    start = parse_local_api_time(times[0])
    output["precision_class"] = [
        precision_class_for_lead((parse_local_api_time(value) - start).total_seconds() / 3600, module)
        for value in times
    ]
    return output


def daily_precision_class(classes: list[str]) -> str:
    unique = {value for value in classes if value}
    if len(unique) == 1:
        return next(iter(unique))
    if unique:
        return "mixed"
    return "undetermined"


def _values_for_indices(hourly: dict, key: str, indices: list[int]) -> list[float]:
    values = hourly.get(key) or []
    result = []
    for index in indices:
        if index >= len(values) or values[index] is None:
            continue
        result.append(float(values[index]))
    return result


def _complete_values_for_indices(hourly: dict, key: str, indices: list[int]) -> list[float]:
    values = hourly.get(key)
    if not isinstance(values, list) or any(index >= len(values) or values[index] is None for index in indices):
        return []
    return [float(values[index]) for index in indices]


def safe_mean(values: list[float]) -> float | None:
    return round(mean(values), 3) if values else None


def safe_sum(values: list[float]) -> float | None:
    return round(sum(values), 3) if values else None


def circular_mean_degrees(values: list[float]) -> float | None:
    """Vector average for a circular quantity such as a wind direction.

    A plain arithmetic mean is wrong here: 350 deg and 10 deg average to 180 deg
    arithmetically but to 0 deg in reality.  When the resultant vector length is
    degenerate the direction is undefined and ``None`` is returned instead of a
    meaningless number.
    """
    cleaned = [float(value) for value in values if isinstance(value, (int, float))]
    if not cleaned:
        return None
    if len(cleaned) == 1:
        return round(cleaned[0] % 360.0, 3) % 360.0
    x = sum(math.cos(math.radians(value)) for value in cleaned)
    y = sum(math.sin(math.radians(value)) for value in cleaned)
    if abs(x) < 1e-9 and abs(y) < 1e-9:
        return None
    return round(math.degrees(math.atan2(y, x)) % 360.0, 3) % 360.0


def circular_resultant_length(values: list[float]) -> float | None:
    """Resultant vector length R in [0, 1]; 1 means perfectly aligned."""
    cleaned = [float(value) for value in values if isinstance(value, (int, float))]
    if not cleaned:
        return None
    x = sum(math.cos(math.radians(value)) for value in cleaned)
    y = sum(math.sin(math.radians(value)) for value in cleaned)
    return round(math.hypot(x, y) / len(cleaned), 3)


def wind_direction_statistics(values: list[float]) -> dict:
    return {
        "mean_deg": circular_mean_degrees(values),
        "resultant_length": circular_resultant_length(values),
        "circular_averaging": True,
        "convention": "degrees from true north, meteorological origin direction",
        "sample_count": len([value for value in values if isinstance(value, (int, float))]),
    }


def variable_availability(hourly: dict, variables) -> dict[str, str]:
    """Classify each requested variable against the returned hourly payload.

    A returned-but-all-null array is an unavailable capability, not a value; it is
    reported as such so no downstream layer can silently invent a replacement.
    """
    times = hourly.get("time") if isinstance(hourly.get("time"), list) else []
    result: dict[str, str] = {}
    for variable in variables:
        values = hourly.get(variable)
        if not isinstance(values, list):
            result[variable] = VARIABLE_STATUS_MISSING
            continue
        if times and len(values) != len(times):
            result[variable] = VARIABLE_STATUS_LENGTH_MISMATCH
            continue
        present = [value for value in values if value is not None]
        if not present:
            result[variable] = VARIABLE_STATUS_NULL_ARRAY
        elif len(present) != len(values):
            result[variable] = VARIABLE_STATUS_PARTIAL_NULL
        else:
            result[variable] = VARIABLE_STATUS_OK
    return result


def variable_status_classification(
    availability: dict[str, str],
    *,
    required_variables=(),
    optional_variables=(),
) -> dict[str, str]:
    """Map raw availability to the status vocabulary published in status.json."""
    required = set(required_variables)
    optional = set(optional_variables)
    result: dict[str, str] = {}
    for variable, raw in availability.items():
        if raw == VARIABLE_STATUS_OK:
            result[variable] = VARIABLE_STATUS_OK
        elif raw == VARIABLE_STATUS_PARTIAL_NULL:
            result[variable] = VARIABLE_STATUS_PARTIAL
        elif variable in required:
            result[variable] = VARIABLE_STATUS_REQUIRED_UNAVAILABLE
        elif variable in optional:
            result[variable] = VARIABLE_STATUS_OPTIONAL_UNAVAILABLE
        else:
            result[variable] = raw
    return result


def unavailable_variables_for(
    variable_status: dict[str, str],
    *,
    required_variables=(),
    optional_variables=(),
) -> dict:
    required = set(required_variables)
    optional = set(optional_variables)
    status_by_variable = variable_status or {}
    required_unavailable = sorted(
        variable
        for variable, status in status_by_variable.items()
        if variable in required and status not in {VARIABLE_STATUS_OK, VARIABLE_STATUS_PARTIAL}
    )
    optional_unavailable = sorted(
        variable
        for variable, status in status_by_variable.items()
        if variable in optional and status not in {VARIABLE_STATUS_OK, VARIABLE_STATUS_PARTIAL}
    )
    return {
        "required_unavailable_variables": required_unavailable,
        "optional_unavailable_variables": optional_unavailable,
        "unavailable_variables": sorted(set(required_unavailable) | set(optional_unavailable)),
    }


def aggregate_variable_status(variable_statuses: list[dict[str, str]]) -> dict[str, str]:
    """Worst-case aggregation across points for a module-level variable report."""
    order = {
        VARIABLE_STATUS_REQUIRED_UNAVAILABLE: 5,
        VARIABLE_STATUS_MISSING: 4,
        VARIABLE_STATUS_LENGTH_MISMATCH: 4,
        VARIABLE_STATUS_NULL_ARRAY: 4,
        VARIABLE_STATUS_OPTIONAL_UNAVAILABLE: 3,
        VARIABLE_STATUS_PARTIAL_NULL: 2,
        VARIABLE_STATUS_PARTIAL: 2,
        VARIABLE_STATUS_OK: 1,
    }
    result: dict[str, str] = {}
    for item in variable_statuses:
        for variable, status in (item or {}).items():
            current = result.get(variable)
            if current is None or order.get(status, 0) > order.get(current, 0):
                result[variable] = status
    return result


def daily_metrics(hourly: dict, solar_variable: str | None = None) -> list[dict]:
    times = hourly.get("time") or []
    groups: dict[str, list[int]] = {}
    for index, value in enumerate(times):
        day = parse_local_api_time(value).date().isoformat()
        groups.setdefault(day, []).append(index)
    output = []
    for day, indices in sorted(groups.items()):
        temperatures = _values_for_indices(hourly, "temperature_2m", indices)
        night_indices = [index for index in indices if parse_local_api_time(times[index]).hour <= 6 or parse_local_api_time(times[index]).hour >= 20]
        night_temperatures = _values_for_indices(hourly, "temperature_2m", night_indices)
        precipitation = _values_for_indices(hourly, "precipitation", indices)
        rain = _values_for_indices(hourly, "rain", indices)
        snowfall = _values_for_indices(hourly, "snowfall", indices)
        cloud = _values_for_indices(hourly, "cloud_cover", indices)
        cloud_low = _values_for_indices(hourly, "cloud_cover_low", indices)
        cloud_mid = _values_for_indices(hourly, "cloud_cover_mid", indices)
        cloud_high = _values_for_indices(hourly, "cloud_cover_high", indices)
        humidity = _values_for_indices(hourly, "relative_humidity_2m", indices)
        dew_point = _values_for_indices(hourly, "dew_point_2m", indices)
        sunshine = _values_for_indices(hourly, "sunshine_duration", indices)
        shortwave = _values_for_indices(hourly, "shortwave_radiation", indices)
        wind = _values_for_indices(hourly, "wind_speed_10m", indices)
        wind_direction = _values_for_indices(hourly, "wind_direction_10m", indices)
        gust = _values_for_indices(hourly, "wind_gusts_10m", indices)
        classes = [
            (hourly.get("precision_class") or ["undetermined"] * len(times))[index]
            for index in indices
            if index < len(hourly.get("precision_class") or [])
        ]
        item = {
            "date": day,
            "complete": len(indices) == 24 and len(temperatures) == len(indices),
            "temperature_min_c": round(min(temperatures), 3) if temperatures else None,
            "temperature_max_c": round(max(temperatures), 3) if temperatures else None,
            "temperature_mean_c": safe_mean(temperatures),
            "night_min_c": round(min(night_temperatures), 3) if night_temperatures else None,
            "dew_point_mean_c": safe_mean(dew_point),
            "relative_humidity_mean_pct": safe_mean(humidity),
            "precipitation_mm": safe_sum(precipitation),
            "rain_mm": safe_sum(rain),
            "snowfall_cm": safe_sum(snowfall),
            "cloud_cover_mean_pct": safe_mean(cloud),
            "cloud_cover_low_mean_pct": safe_mean(cloud_low),
            "cloud_cover_mid_mean_pct": safe_mean(cloud_mid),
            "cloud_cover_high_mean_pct": safe_mean(cloud_high),
            "wind_speed_mean_kmh": safe_mean(wind),
            "wind_direction_mean_deg": circular_mean_degrees(wind_direction),
            "wind_direction_member_resultant_length": circular_resultant_length(wind_direction),
            "wind_gust_max_kmh": round(max(gust), 3) if gust else None,
            "wind_gust_mean_kmh": safe_mean(gust),
            "precision_class": daily_precision_class(classes),
        }
        if solar_variable == "sunshine_duration" and sunshine:
            item["solar_metric"] = {"variable": solar_variable, "value": round(sum(sunshine), 3), "unit": "seconds"}
        elif solar_variable == "shortwave_radiation" and shortwave:
            item["solar_metric"] = {"variable": solar_variable, "value": round(mean(shortwave), 3), "unit": "W/m² mean"}
        else:
            item["solar_metric"] = None
        output.append(item)
    return output


def trim_incomplete_hourly_rows(hourly: dict, required_variables: list[str]) -> tuple[dict, dict]:
    """Keep only a complete hourly interior; retain an audit of omitted edge rows."""
    output_hourly = copy.deepcopy(hourly)
    times = output_hourly.get("time") if isinstance(output_hourly.get("time"), list) else []
    original_count = len(times)
    leading_count = 0
    leading_variables = set()
    while output_hourly.get("time"):
        missing = [
            variable
            for variable in required_variables
            if isinstance(output_hourly.get(variable), list)
            and output_hourly[variable]
            and output_hourly[variable][0] is None
        ]
        if not missing:
            break
        leading_count += 1
        leading_variables.update(missing)
        for values in output_hourly.values():
            if isinstance(values, list):
                values.pop(0)
    trim_count = 0
    tail_variables = set()
    while output_hourly.get("time"):
        last_index = len(output_hourly["time"]) - 1
        missing = [
            variable
            for variable in required_variables
            if isinstance(output_hourly.get(variable), list)
            and last_index < len(output_hourly[variable])
            and output_hourly[variable][last_index] is None
        ]
        if not missing:
            break
        trim_count += 1
        tail_variables.update(missing)
        for values in output_hourly.values():
            if isinstance(values, list):
                values.pop()
    retained_count = len(output_hourly.get("time") or [])
    audit = {
        "original_timestep_count": original_count,
        "retained_timestep_count": retained_count,
        "leading_missing_rows": leading_count,
        "leading_missing_variables": sorted(leading_variables),
        "trailing_missing_rows": trim_count,
        "trailing_missing_variables": sorted(tail_variables),
        "horizon_status": "TRUNCATED_EDGE_MISSING" if leading_count or trim_count else "COMPLETE",
    }
    return output_hourly, audit


def trim_incomplete_edge_rows(payload: dict, required_variables: list[str]) -> tuple[dict, dict]:
    """Trim API edge rows while preserving the rest of the response metadata."""
    output = copy.deepcopy(payload)
    output["hourly"], audit = trim_incomplete_hourly_rows(output.get("hourly") or {}, required_variables)
    return output, audit


def trim_to_forecast_cutoff(payload: dict, max_date: dt.date | None) -> tuple[dict, dict]:
    """Drop forecast rows after a configured local-date cutoff and audit the drop."""
    output = copy.deepcopy(payload)
    hourly = output.get("hourly") if isinstance(output.get("hourly"), dict) else {}
    times = hourly.get("time") if isinstance(hourly.get("time"), list) else []
    if max_date is None:
        return output, {
            "forecast_cutoff_applied": False,
            "forecast_cutoff_date": None,
            "rows_dropped_after_cutoff": 0,
        }
    keep_indices = []
    invalid_timestamps = []
    for index, value in enumerate(times):
        try:
            keep = parse_local_api_time(value).date() <= max_date
        except (TypeError, ValueError):
            keep = False
            invalid_timestamps.append(index)
        if keep:
            keep_indices.append(index)
    for key, values in list(hourly.items()):
        if isinstance(values, list):
            hourly[key] = [values[index] for index in keep_indices if index < len(values)]
    output["hourly"] = hourly
    return output, {
        "forecast_cutoff_applied": True,
        "forecast_cutoff_date": max_date.isoformat(),
        "rows_before_cutoff": len(times),
        "rows_retained_through_cutoff": len(keep_indices),
        "rows_dropped_after_cutoff": len(times) - len(keep_indices),
        "invalid_timestamp_rows_dropped": invalid_timestamps,
    }


def response_meta(payload: dict) -> dict:
    return {
        "grid_coordinate": {
            "latitude": payload.get("latitude"),
            "longitude": payload.get("longitude"),
        },
        "returned_elevation": payload.get("elevation"),
        "timezone": payload.get("timezone"),
        "utc_offset_seconds": payload.get("utc_offset_seconds"),
        "generationtime_ms": payload.get("generationtime_ms"),
        "returned_model": payload.get("model"),
        "returned_model_id": payload.get("model_id"),
        "model_run_initialization": payload.get("model_run_initialization"),
    }


def validate_payload(
    payload: dict,
    point: dict,
    expected_model: str,
    required_variables: list[str],
    grid_limit_km: float,
    requested_elevation: str = "nan",
    model_run_initialization: str | None = None,
    accepted_model_values: tuple[str, ...] = (),
    accepted_model_ids: tuple[str, ...] = (),
) -> dict:
    response = response_meta(payload)
    grid = response["grid_coordinate"]
    distance = None
    distance_pass = False
    if valid_coordinate(grid.get("latitude"), grid.get("longitude")):
        distance = haversine_km(point["latitude"], point["longitude"], grid["latitude"], grid["longitude"])
        distance_pass = distance <= grid_limit_km
    hourly = payload.get("hourly") if isinstance(payload.get("hourly"), dict) else {}
    times = hourly.get("time") if isinstance(hourly.get("time"), list) else []
    missing_variables = [name for name in ["time", *required_variables] if name not in hourly]
    length_mismatch = [name for name in required_variables if name in hourly and len(hourly[name]) != len(times)]
    null_values = [
        name
        for name in required_variables
        if name in hourly and any(value is None for value in hourly[name])
    ]
    coordinate_pass = valid_coordinate(point["latitude"], point["longitude"]) and valid_coordinate(grid.get("latitude"), grid.get("longitude"))
    timezone_pass = payload.get("timezone") == TIMEZONE_NAME and payload.get("utc_offset_seconds") == 28800
    returned_model = payload.get("model")
    accepted_models = {expected_model.lower(), *(value.lower() for value in accepted_model_values)}
    model_pass = returned_model is None or str(returned_model).lower() in accepted_models
    returned_model_id = payload.get("model_id")
    model_id_pass = not returned_model_id or not accepted_model_ids or str(returned_model_id).lower() in {
        value.lower() for value in accepted_model_ids
    }
    elevation_pass = requested_elevation == "nan"
    data_pass = bool(times) and not missing_variables and not length_mismatch and not null_values
    checks = {
        "coordinate_check": "PASS" if coordinate_pass else "FAIL",
        "distance_check": "PASS" if distance_pass else "FAIL",
        "timezone_check": "PASS" if timezone_pass else "FAIL",
        "model_check": "PASS" if model_pass else "FAIL",
        "model_id_check": "PASS" if model_id_pass else "FAIL",
        "elevation_check": "PASS" if elevation_pass else "FAIL",
        "data_check": "PASS" if data_pass else "FAIL",
    }
    valid = all(value == "PASS" for value in checks.values())
    reason = None
    if not valid:
        reasons = []
        if not coordinate_pass:
            reasons.append("COORDINATE_INVALID")
        if not distance_pass:
            reasons.append("GRID_REPRESENTATIVENESS_FAIL")
        if not timezone_pass:
            reasons.append("TIMEZONE_MISMATCH")
        if not model_pass:
            reasons.append("MODEL_MISMATCH")
        if not model_id_pass:
            reasons.append("MODEL_ID_MISMATCH")
        if missing_variables:
            reasons.append("MISSING_DATA:" + ",".join(missing_variables))
        if length_mismatch:
            reasons.append("ARRAY_LENGTH_MISMATCH:" + ",".join(length_mismatch))
        if null_values:
            reasons.append("NULL_DATA:" + ",".join(null_values))
        reason = ";".join(reasons) or "INVALID"
    return {
        "valid": valid,
        "grid_distance_km": round(distance, 3) if distance is not None else None,
        "grid_distance_limit_km": grid_limit_km,
        "distance_check": checks["distance_check"],
        "timezone_check": checks["timezone_check"],
        "model_check": checks["model_check"],
        "model_id_check": checks["model_id_check"],
        "coordinate_check": checks["coordinate_check"],
        "elevation_check": checks["elevation_check"],
        "elevation_mode": "native_model_grid" if requested_elevation == "nan" else "requested_elevation",
        "requested_elevation": requested_elevation,
        "returned_elevation": response.get("returned_elevation"),
        "data_check": checks["data_check"],
        "missing_variables": missing_variables,
        "array_length_mismatch": length_mismatch,
        "null_data_variables": null_values,
        "model_run_initialization": model_run_initialization or response.get("model_run_initialization"),
        "final_status": "PASS" if valid else "INVALID",
        "reason": reason,
    }


def degraded_variable_list(
    requested_variables: list[str],
    optional_variables,
) -> list[str]:
    """Drop unsupported optional variables while keeping the solar alternative.

    This is the only sanctioned degradation path: the optional variable set is
    removed and recorded, no other model's value is substituted, and the required
    variables stay in the request.
    """
    optional = set(optional_variables)
    result = []
    for variable in requested_variables:
        if variable == "sunshine_duration":
            result.append(HRES_FALLBACK_SOLAR)
        elif variable in optional:
            continue
        else:
            result.append(variable)
    return result


def request_payload(
    client: ApiClient,
    *,
    endpoint: str,
    params: dict[str, object],
    variables: list[str],
    label: str,
    optional_variables=(),
) -> tuple[dict, str, str, list[str]]:
    requested_variables = list(variables)
    solar_variable = "sunshine_duration" if "sunshine_duration" in requested_variables else None
    try:
        payload, url = client.get_json(endpoint, {**params, "hourly": ",".join(requested_variables)}, label)
        return payload, url, solar_variable or "", []
    except OpenMeteoError as error:
        if not is_variable_error(error):
            raise
        last_error = error
    if solar_variable:
        fallback = [HRES_FALLBACK_SOLAR if value == solar_variable else value for value in requested_variables]
        try:
            log(f"[{label}] SOLAR VARIABLE FALLBACK: {solar_variable} -> {HRES_FALLBACK_SOLAR}")
            payload, url = client.get_json(
                endpoint, {**params, "hourly": ",".join(fallback)}, label + ":SOLAR_FALLBACK"
            )
            return payload, url, HRES_FALLBACK_SOLAR, []
        except OpenMeteoError as error:
            if not is_variable_error(error):
                raise
            last_error = error
    # An optional variable that this model/endpoint does not serve must not take
    # the whole model module down.  Record the drop explicitly and retry once.
    reduced = degraded_variable_list(requested_variables, optional_variables)
    if reduced and reduced != requested_variables:
        dropped = [value for value in requested_variables if value not in reduced]
        replaced_solar = solar_variable and "sunshine_duration" not in reduced
        try:
            log(f"[{label}] OPTIONAL VARIABLE DROP: {','.join(dropped)}")
            payload, url = client.get_json(
                endpoint, {**params, "hourly": ",".join(reduced)}, label + ":OPTIONAL_DROP"
            )
            return payload, url, HRES_FALLBACK_SOLAR if replaced_solar else (solar_variable or ""), dropped
        except OpenMeteoError as error:
            if not is_variable_error(error):
                raise
            last_error = error
    raise last_error


def invalid_record(
    *,
    point: dict,
    source: str,
    endpoint: str,
    model: str,
    request_params: dict,
    reason: str,
    error: OpenMeteoError | None = None,
    model_run_initialization: str | None = None,
) -> dict:
    return {
        "point_id": point.get("id"),
        "point": {
            "name": point.get("name"),
            "region": point.get("region"),
            "status": point.get("status"),
            "latitude": point.get("latitude"),
            "longitude": point.get("longitude"),
        },
        "status": "INVALID",
        "source": source,
        "endpoint": endpoint,
        "model": model,
        "request": {
            "coordinate": {"latitude": point.get("latitude"), "longitude": point.get("longitude")},
            "parameters": request_params,
        },
        "response": None,
        "qa": {
            "valid": False,
            "final_status": "INVALID",
            "reason": reason,
            "model_run_initialization": model_run_initialization,
        },
        "error": {
            "reason": reason,
            "http_status": error.status_code if error else None,
        },
    }


def fetch_point(
    client: ApiClient,
    *,
    point: dict,
    source: str,
    endpoint: str,
    model: str,
    params: dict[str, object],
    variables: list[str],
    required_variables: list[str],
    grid_limit_km: float,
    log_label: str,
    precision_module: str | None = None,
    model_run_initialization: str | None = None,
    accepted_model_values: tuple[str, ...] = (),
    accepted_model_ids: tuple[str, ...] = (),
    max_forecast_date: dt.date | None = None,
    optional_variables: tuple[str, ...] | list[str] = (),
) -> dict:
    try:
        payload, url, solar_variable, degraded_variables = request_payload(
            client,
            endpoint=endpoint,
            params=params,
            variables=variables,
            label=log_label,
            optional_variables=optional_variables,
        )
    except OpenMeteoError as error:
        log(f"[{log_label}] FETCH FAILED: {error.reason}")
        return invalid_record(
            point=point,
            source=source,
            endpoint=endpoint,
            model=model,
            request_params={**params, "hourly": ",".join(variables)},
            reason="OPEN_METEO_REQUEST_FAILED:" + error.reason,
            error=error,
            model_run_initialization=model_run_initialization,
        )
    selected_required_variables = [value for value in [*required_variables, solar_variable] if value]
    payload, cutoff_audit = trim_to_forecast_cutoff(payload, max_forecast_date)
    payload, completeness = trim_incomplete_edge_rows(payload, selected_required_variables)
    completeness.update(cutoff_audit)
    qa = validate_payload(
        payload,
        point,
        expected_model=model,
        required_variables=selected_required_variables,
        grid_limit_km=grid_limit_km,
        requested_elevation=str(params.get("elevation", "")),
        model_run_initialization=model_run_initialization,
        accepted_model_values=accepted_model_values,
        accepted_model_ids=accepted_model_ids,
    )
    qa.update(completeness)
    response = response_meta(payload)
    response.update(completeness)
    response["retrieval_time"] = iso_utc(dt.datetime.now(UTC))
    response["endpoint_url"] = url
    response["model_run_initialization"] = model_run_initialization or response.get("model_run_initialization")
    payload_hourly = payload.get("hourly") if isinstance(payload.get("hourly"), dict) else {}
    raw_availability = variable_availability(payload_hourly, variables)
    variable_status = variable_status_classification(
        raw_availability,
        required_variables=required_variables,
        optional_variables=optional_variables,
    )
    for variable in degraded_variables:
        variable_status[variable] = (
            VARIABLE_STATUS_REQUIRED_UNAVAILABLE
            if variable in set(required_variables)
            else VARIABLE_STATUS_OPTIONAL_UNAVAILABLE
        )
        raw_availability.setdefault(variable, VARIABLE_STATUS_MISSING)
    availability = unavailable_variables_for(
        variable_status,
        required_variables=required_variables,
        optional_variables=optional_variables,
    )
    if solar_variable == HRES_FALLBACK_SOLAR and "sunshine_duration" in variables:
        variable_status["sunshine_duration"] = VARIABLE_STATUS_OPTIONAL_UNAVAILABLE
        variable_status[HRES_FALLBACK_SOLAR] = VARIABLE_STATUS_OK
        raw_availability["sunshine_duration"] = VARIABLE_STATUS_MISSING
        raw_availability[HRES_FALLBACK_SOLAR] = VARIABLE_STATUS_OK
        availability = unavailable_variables_for(
            variable_status,
            required_variables=required_variables,
            optional_variables=optional_variables,
        )
    record = {
        "point_id": point.get("id"),
        "point": {
            "name": point.get("name"),
            "region": point.get("region"),
            "status": point.get("status"),
            "latitude": point.get("latitude"),
            "longitude": point.get("longitude"),
        },
        "status": "PASS" if qa["valid"] else "INVALID",
        "source": source,
        "endpoint": endpoint,
        "model": model,
        "request": {
            "coordinate": {"latitude": point.get("latitude"), "longitude": point.get("longitude")},
            "parameters": {**params, "hourly": url.split("hourly=", 1)[1].split("&", 1)[0] if "hourly=" in url else ",".join(variables)},
        },
        "response": response,
        "qa": qa,
        "solar_variable": solar_variable,
        "variable_status": variable_status,
        "requested_variables": list(variables),
        "required_variables": list(required_variables),
        "optional_variables": list(optional_variables),
        "degraded_variables": list(degraded_variables),
        "raw_variable_availability": raw_availability,
        **availability,
    }
    if qa["valid"]:
        hourly = payload["hourly"]
        if precision_module:
            hourly = add_precision_classes(hourly, precision_module)
        record["hourly"] = hourly
        record["daily"] = daily_metrics(hourly, solar_variable)
        operation = (
            "SINGLE_RUN"
            if ":SINGLE_RUN " in log_label
            else "SPATIAL"
            if log_label.endswith(":SPATIAL")
            else log_label.split(":", 1)[1] if ":" in log_label else log_label
        )
        log(f"[{point['id']}] {operation} FETCH OK")
        log(f"[{point['id']}] GRID QA {'PASS' if qa['valid'] else 'FAIL'}")
    else:
        log(f"[{point['id']}] GRID QA FAIL: {qa.get('reason')}")
    return record


def module_status(records: list[dict], expected_count: int) -> str:
    if expected_count > 0 and len(records) == expected_count and all(record.get("status") == "PASS" for record in records):
        return "OK"
    return "FAILED"


def module_header(name: str, generated_at: str, data_date: str, status: str, **extra: object) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "module": name,
        "status": status,
        "generated_at": generated_at,
        "data_date": data_date,
        **extra,
    }


def base_weather_params(point: dict, **extra: object) -> dict[str, object]:
    return {
        "latitude": point["latitude"],
        "longitude": point["longitude"],
        "timezone": TIMEZONE_NAME,
        "cell_selection": "nearest",
        "elevation": "nan",
        **extra,
    }


def site_hres_grid_qa_limit_km(config: dict) -> float:
    """Return the ECMWF HRES grid-representativeness tolerance for this site.

    ``HRES_GRID_QA_LIMIT_KM`` is the default and remains in force whenever the
    site does not declare its own value, so a site that omits ``grid_qa``
    behaves exactly as before. The tolerance is site-specific because the
    Open-Meteo ``/v1/ecmwf`` endpoint answers on a 0.25 degree grid: the
    worst-case offset from a requested point to its nearest returned cell centre
    is the cell half-diagonal, and its east-west leg shrinks with
    ``cos(latitude)`` (about 16.7 km at 48 degrees N, about 18.1 km at 33
    degrees N). The default was set for a high-latitude site, so a low-latitude
    site whose points are legitimately placed can still land outside it.
    Declaring the wider tolerance per site keeps that widening visible in
    ``config/points.json`` instead of loosening the gate for every site.
    """
    declared_block = config.get(SITE_GRID_QA_CONFIG_KEY)
    if declared_block is None:
        return HRES_GRID_QA_LIMIT_KM
    if not isinstance(declared_block, dict):
        raise ValueError(f"{SITE_GRID_QA_CONFIG_KEY} must be an object")
    declared = declared_block.get(SITE_HRES_GRID_QA_LIMIT_KEY)
    if declared is None:
        return HRES_GRID_QA_LIMIT_KM
    try:
        limit = float(declared)
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"{SITE_GRID_QA_CONFIG_KEY}.{SITE_HRES_GRID_QA_LIMIT_KEY} must be a number, got {declared!r}"
        ) from error
    if not limit > 0:
        raise ValueError(
            f"{SITE_GRID_QA_CONFIG_KEY}.{SITE_HRES_GRID_QA_LIMIT_KEY} must be positive, got {declared!r}"
        )
    return limit


def run_hres(config: dict, client: ApiClient, generated_at: str, data_date: str) -> dict:
    points = active_points(config)
    grid_limit_km = site_hres_grid_qa_limit_km(config)
    records = []
    by_id = {}
    for point_id, point in points.items():
        record = fetch_point(
            client,
            point=point,
            source="Open-Meteo",
            endpoint=OPEN_METEO_ENDPOINTS["hres"],
            model="ECMWF IFS HRES 9 km",
            params=base_weather_params(point, forecast_days=15),
            variables=HRES_VARIABLES,
            required_variables=HRES_REQUIRED_VARIABLES,
            optional_variables=HRES_OPTIONAL_VARIABLES,
            grid_limit_km=grid_limit_km,
            log_label=f"{point_id}:HRES",
            precision_module="hres",
            max_forecast_date=point_forecast_end_date(point),
        )
        records.append(record)
        by_id[point_id] = record
    status = module_status(records, len(points))
    variable_status = aggregate_variable_status(
        [record.get("variable_status") or {} for record in records]
    )
    unavailable = unavailable_variables_for(
        variable_status,
        required_variables=HRES_REQUIRED_VARIABLES,
        optional_variables=HRES_OPTIONAL_VARIABLES,
    )
    return module_header(
        "hres",
        generated_at,
        data_date,
        status,
        endpoint=OPEN_METEO_ENDPOINTS["hres"],
        model="ECMWF IFS HRES 9 km",
        native_resolution="9 km",
        precision_policy=PRECISION_POLICIES["hres"],
        interpolation_note="Open-Meteo returns an hourly series; after 90 h and 144 h it represents coarser native IFS time steps.",
        requested_variables=list(HRES_VARIABLES),
        required_variables=list(HRES_REQUIRED_VARIABLES),
        optional_variables=list(HRES_OPTIONAL_VARIABLES),
        variable_status=variable_status,
        **unavailable,
        points=by_id,
        excluded_points=excluded_points(config),
        successful_points=sum(record.get("status") == "PASS" for record in records),
        failed_points=sum(record.get("status") != "PASS" for record in records),
    )


def history_date_range(
    completed_date: dt.date,
    year: int,
    start_month_day: str = "08-25",
) -> tuple[str, str] | None:
    try:
        start_month, start_day = (int(value) for value in start_month_day.split("-", 1))
        start = dt.date(year, start_month, start_day)
        current_start = dt.date(2026, start_month, start_day)
    except (TypeError, ValueError) as error:
        raise ValueError(f"invalid history start month-day: {start_month_day}") from error
    if completed_date < current_start:
        return None
    try:
        end = dt.date(year, completed_date.month, completed_date.day)
    except ValueError:
        end = dt.date(year, completed_date.month, 28)
    return start.isoformat(), end.isoformat()


def history_cache_namespace(config: dict) -> str:
    """Keep historical caches separated per configured namespace."""
    return str(config.get("namespace") or "siguniang_jiuzhaigou")


def history_cache_path(
    config: dict,
    year: int,
    point_id: str,
    cache_dir: Path | None = None,
) -> Path:
    """Return the stable cache path for one namespace/year/VERIFIED point."""
    root = Path(cache_dir) if cache_dir is not None else HISTORY_CACHE_DIR
    return root / history_cache_namespace(config) / str(year) / f"{point_id}.json"


def history_cache_relative_path(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def history_cache_daily(record: dict) -> list[dict]:
    """Extract daily values for the compact cache without retaining hourly arrays."""
    daily = record.get("daily")
    if isinstance(daily, list):
        return copy.deepcopy(daily)
    hourly = record.get("hourly")
    if isinstance(hourly, dict):
        return daily_metrics(hourly, record.get("solar_variable"))
    return []


def history_cache_identity(config: dict, point: dict, year: int, record: dict) -> dict:
    """Capture every input and returned-grid value that gives a cache its meaning."""
    request = record.get("request") or {}
    parameters = request.get("parameters") or {}
    response = record.get("response") or {}
    qa = record.get("qa") or {}
    grid = response.get("grid_coordinate")
    grid_key = record_grid_cell_key(record)
    return {
        "namespace": history_cache_namespace(config),
        "year": int(year),
        "point_id": point.get("id"),
        "source": record.get("source", "Open-Meteo"),
        "endpoint": record.get("endpoint", OPEN_METEO_ENDPOINTS["history"]),
        "model": record.get("model", HISTORY_MODEL),
        "model_parameter": parameters.get("models", HISTORY_MODEL_PARAMETER),
        "requested_coordinate": request.get("coordinate") or {
            "latitude": point.get("latitude"),
            "longitude": point.get("longitude"),
        },
        "returned_grid_coordinate": copy.deepcopy(grid),
        "returned_elevation": response.get("returned_elevation"),
        "grid_distance_km": qa.get("grid_distance_km"),
        "grid_distance_limit_km": qa.get("grid_distance_limit_km", HISTORY_GRID_QA_LIMIT_KM),
        "grid_cell_key": grid_key,
        "cell_selection": parameters.get("cell_selection"),
        "elevation": parameters.get("elevation"),
        "timezone": parameters.get("timezone") or response.get("timezone"),
        "utc_offset_seconds": response.get("utc_offset_seconds"),
        "solar_variable": record.get("solar_variable"),
    }


def history_cache_key(identity: dict) -> str:
    return ":".join(
        str(identity.get(key) or "UNKNOWN")
        for key in ("namespace", "year", "point_id", "grid_cell_key")
    )


def history_cache_record_metadata(point: dict, record: dict) -> dict:
    return {
        "point": {
            "name": point.get("name"),
            "region": point.get("region"),
            "status": point.get("status"),
            "latitude": point.get("latitude"),
            "longitude": point.get("longitude"),
        },
        "source": record.get("source", "Open-Meteo"),
        "endpoint": record.get("endpoint", OPEN_METEO_ENDPOINTS["history"]),
        "model": record.get("model", HISTORY_MODEL),
        "request": copy.deepcopy(record.get("request") or {}),
        "response": copy.deepcopy(record.get("response") or {}),
        "qa": copy.deepcopy(record.get("qa") or {}),
        "solar_variable": record.get("solar_variable"),
    }


def history_cache_from_record(
    config: dict,
    point: dict,
    year: int,
    record: dict,
    requested_start: str,
    requested_end: str,
    *,
    mode: str,
) -> dict:
    daily = history_cache_daily(record)
    identity = history_cache_identity(config, point, year, record)
    retrieval_time = (
        (record.get("response") or {}).get("retrieval_time")
        or iso_utc(dt.datetime.now(UTC))
    )
    return {
        "cache_schema_version": "1.0.0",
        "schema_version": SCHEMA_VERSION,
        "cache_kind": "historical_daily_weather",
        "cache_key": history_cache_key(identity),
        "namespace": history_cache_namespace(config),
        "year": int(year),
        "point_id": point.get("id"),
        "identity": identity,
        "record_metadata": history_cache_record_metadata(point, record),
        "daily": daily,
        "cached_dates": sorted({day.get("date") for day in daily if day.get("date")}),
        "date_range": {
            "start_date": min((day["date"] for day in daily if day.get("date")), default=None),
            "end_date": max((day["date"] for day in daily if day.get("date")), default=None),
        },
        "retrievals": [{
            "retrieved_at": retrieval_time,
            "requested_start_date": requested_start,
            "requested_end_date": requested_end,
            "mode": mode,
            "status": "PASS",
        }],
        "last_retrieval_time": retrieval_time,
    }


def _history_cache_identity_mismatches(
    cache: dict,
    config: dict,
    point: dict,
    year: int,
) -> list[str]:
    identity = cache.get("identity")
    if not isinstance(identity, dict):
        return ["CACHE_IDENTITY_MISSING"]
    expected_coordinate = {
        "latitude": point.get("latitude"),
        "longitude": point.get("longitude"),
    }
    expected = {
        "namespace": history_cache_namespace(config),
        "year": int(year),
        "point_id": point.get("id"),
        "source": "Open-Meteo",
        "endpoint": OPEN_METEO_ENDPOINTS["history"],
        "model": HISTORY_MODEL,
        "model_parameter": HISTORY_MODEL_PARAMETER,
        "requested_coordinate": expected_coordinate,
        "cell_selection": "nearest",
        "elevation": "nan",
        "timezone": TIMEZONE_NAME,
    }
    mismatches = []
    for key, expected_value in expected.items():
        if identity.get(key) != expected_value:
            mismatches.append(key)
    grid = identity.get("returned_grid_coordinate")
    if not valid_coordinate((grid or {}).get("latitude"), (grid or {}).get("longitude")):
        mismatches.append("returned_grid_coordinate")
    else:
        expected_grid_key = f"{float(grid['latitude']):.6f},{float(grid['longitude']):.6f}"
        if identity.get("grid_cell_key") != expected_grid_key:
            mismatches.append("grid_cell_key")
    distance = identity.get("grid_distance_km")
    limit = identity.get("grid_distance_limit_km", HISTORY_GRID_QA_LIMIT_KM)
    if (
        not isinstance(distance, (int, float))
        or isinstance(distance, bool)
        or not math.isfinite(float(distance))
        or distance < 0
        or not isinstance(limit, (int, float))
        or isinstance(limit, bool)
        or not math.isfinite(float(limit))
        or limit < 0
        or distance > limit
    ):
        mismatches.append("grid_distance_km")
    returned_elevation = identity.get("returned_elevation")
    if (
        not isinstance(returned_elevation, (int, float))
        or isinstance(returned_elevation, bool)
        or not math.isfinite(float(returned_elevation))
    ):
        mismatches.append("returned_elevation")
    if identity.get("utc_offset_seconds") != 28800:
        mismatches.append("utc_offset_seconds")
    if identity.get("solar_variable") not in {None, "sunshine_duration", "shortwave_radiation"}:
        mismatches.append("solar_variable")
    if cache.get("cache_key") != history_cache_key(identity):
        mismatches.append("cache_key")
    qa = cache.get("record_metadata", {}).get("qa") or cache.get("qa") or {}
    if qa.get("final_status") != "PASS":
        mismatches.append("qa_final_status")
    daily = cache.get("daily")
    if not isinstance(daily, list):
        mismatches.append("daily")
    else:
        seen_dates = set()
        for day in daily:
            day_date = day.get("date") if isinstance(day, dict) else None
            if not isinstance(day_date, str):
                mismatches.append("daily_date")
                continue
            try:
                dt.date.fromisoformat(day_date)
            except ValueError:
                mismatches.append("daily_date")
            else:
                if day_date[:4] != str(year):
                    mismatches.append("daily_year")
            if day_date in seen_dates:
                mismatches.append("duplicate_daily_date")
            seen_dates.add(day_date)
    return sorted(set(mismatches))


def load_history_cache(
    config: dict,
    point: dict,
    year: int,
    cache_dir: Path | None = None,
) -> tuple[dict | None, dict]:
    """Load and validate one cache file; invalid identity is never silently used."""
    path = history_cache_path(config, year, point["id"], cache_dir)
    info = {
        "path": history_cache_relative_path(path),
        "status": "MISS",
        "identity_mismatches": [],
    }
    if not path.is_file():
        return None, info
    try:
        with path.open(encoding="utf-8") as handle:
            cache = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        info.update({"status": "INVALID", "identity_mismatches": [f"CACHE_READ_FAILED:{type(error).__name__}"]})
        return None, info
    mismatches = _history_cache_identity_mismatches(cache, config, point, year)
    if mismatches:
        info.update({"status": "INVALID", "identity_mismatches": mismatches})
        return None, info
    info["status"] = "HIT"
    info["cache_key"] = cache.get("cache_key")
    info["cached_dates"] = len(cache.get("daily") or [])
    return cache, info


def _history_date_list(start_date: str, end_date: str) -> list[str]:
    start = dt.date.fromisoformat(start_date)
    end = dt.date.fromisoformat(end_date)
    if end < start:
        return []
    return [
        (start + dt.timedelta(days=offset)).isoformat()
        for offset in range((end - start).days + 1)
    ]


def _history_missing_date_ranges(
    start_date: str,
    end_date: str,
    cached_days: list[dict],
) -> list[tuple[str, str]]:
    cached_dates = {
        day.get("date")
        for day in cached_days
        if isinstance(day, dict) and day.get("complete") and isinstance(day.get("date"), str)
    }
    missing = [
        value for value in _history_date_list(start_date, end_date)
        if value not in cached_dates
    ]
    ranges = []
    for value in missing:
        if not ranges:
            ranges.append([value, value])
            continue
        previous = dt.date.fromisoformat(ranges[-1][1])
        current = dt.date.fromisoformat(value)
        if current == previous + dt.timedelta(days=1):
            ranges[-1][1] = value
        else:
            ranges.append([value, value])
    return [(start, end) for start, end in ranges]


def _history_cache_merge_daily(cache: dict, new_daily: list[dict]) -> None:
    by_date = {
        day.get("date"): copy.deepcopy(day)
        for day in cache.get("daily", [])
        if isinstance(day, dict) and isinstance(day.get("date"), str)
    }
    for day in new_daily:
        if isinstance(day, dict) and isinstance(day.get("date"), str):
            by_date[day["date"]] = copy.deepcopy(day)
    cache["daily"] = [by_date[key] for key in sorted(by_date)]
    cache["cached_dates"] = sorted(by_date)
    cache["date_range"] = {
        "start_date": min(by_date) if by_date else None,
        "end_date": max(by_date) if by_date else None,
    }


def _history_cache_record(
    config: dict,
    point: dict,
    year: int,
    cache: dict,
    requested_start: str,
    requested_end: str,
    info: dict,
) -> dict:
    metadata = cache.get("record_metadata") or {}
    daily = [
        copy.deepcopy(day)
        for day in cache.get("daily", [])
        if isinstance(day, dict)
        and isinstance(day.get("date"), str)
        and requested_start <= day["date"] <= requested_end
    ]
    expected = _history_date_list(requested_start, requested_end)
    available = {day["date"] for day in daily if day.get("complete")}
    missing = [value for value in expected if value not in available]
    qa = copy.deepcopy(metadata.get("qa") or {})
    qa["cache_check"] = {
        "status": "PASS" if not missing and info.get("status") not in {"INVALID", "FAILED"} else "INVALID",
        "cache_path": info.get("path"),
        "cache_key": cache.get("cache_key"),
        "requested_start_date": requested_start,
        "requested_end_date": requested_end,
        "missing_dates": missing,
        "identity_mismatches": info.get("identity_mismatches", []),
    }
    record_status = "PASS" if not missing and info.get("status") not in {"INVALID", "FAILED"} else "INVALID"
    if record_status != "PASS":
        qa["valid"] = False
        qa["final_status"] = "INVALID"
        qa["reason"] = (
            "HISTORY_CACHE_IDENTITY_MISMATCH"
            if info.get("status") == "INVALID" and info.get("identity_mismatches")
            else "HISTORY_CACHE_MISSING_DATES"
        )
    else:
        qa.setdefault("valid", True)
        qa.setdefault("final_status", "PASS")
    response = copy.deepcopy(metadata.get("response") or {})
    request = copy.deepcopy(metadata.get("request") or {})
    return {
        "point_id": point.get("id"),
        "point": copy.deepcopy(metadata.get("point") or point),
        "status": record_status,
        "source": metadata.get("source", "Open-Meteo"),
        "endpoint": metadata.get("endpoint", OPEN_METEO_ENDPOINTS["history"]),
        "model": metadata.get("model", HISTORY_MODEL),
        "request": request,
        "response": response,
        "qa": qa,
        "solar_variable": metadata.get("solar_variable"),
        "daily": daily,
        "history_cache": {
            **copy.deepcopy(info),
            "requested_start_date": requested_start,
            "requested_end_date": requested_end,
            "cached_start_date": (cache.get("date_range") or {}).get("start_date"),
            "cached_end_date": (cache.get("date_range") or {}).get("end_date"),
            "cached_complete_dates": len(available),
            "missing_dates": missing,
        },
    }


def _mark_history_cache_record_invalid(record: dict, info: dict, reason: str) -> dict:
    result = copy.deepcopy(record)
    result["status"] = "INVALID"
    result["history_cache"] = copy.deepcopy(info)
    result.setdefault("qa", {})["valid"] = False
    result["qa"]["final_status"] = "INVALID"
    result["qa"]["reason"] = reason
    result.setdefault("error", {})["reason"] = reason
    return result


def history_cache_required_range(
    config: dict,
    point: dict,
    year: int,
    completed_date: dt.date,
    forward_anchor_date: dt.date | None = None,
) -> tuple[str, str] | None:
    """Return the union needed by history comparison and (optionally) forward paths."""
    region_config = config.get("regions", {}).get(point["region"], {})
    history_start = region_config.get(
        "history_start_month_day",
        config.get("history_start_month_day", "08-25"),
    )
    history_range = history_date_range(completed_date, year, history_start)
    ranges = [history_range] if history_range else []
    if (
        forward_anchor_date is not None
        and year in HISTORY_FORWARD_YEARS
    ):
        forward_windows = history_forward_windows_for_year(forward_anchor_date, year)
        forward_ranges = [
            (item.get("start_date"), item.get("end_date"))
            for item in forward_windows.values()
            if item.get("status") == "OK" and item.get("start_date") and item.get("end_date")
        ]
        ranges.extend(forward_ranges)
    ranges = [(start, end) for start, end in ranges if start and end]
    if not ranges:
        return None
    return min(start for start, _ in ranges), max(end for _, end in ranges)


def history_cache_record_or_fetch(
    config: dict,
    client: ApiClient,
    point: dict,
    year: int,
    requested_start: str,
    requested_end: str,
    *,
    refresh_history: bool = False,
    cache_dir: Path | None = None,
    log_label: str,
) -> dict:
    """Read a daily cache and fetch only missing contiguous dates from Open-Meteo."""
    path = history_cache_path(config, year, point["id"], cache_dir)
    cache, load_info = load_history_cache(config, point, year, cache_dir)
    original_cache = cache
    if load_info.get("status") == "INVALID" and not refresh_history:
        info = {
            **load_info,
            "status": "INVALID",
            "requested_start_date": requested_start,
            "requested_end_date": requested_end,
        }
        return _history_cache_record(
            config,
            point,
            year,
            {"record_metadata": {}, "daily": [], "cache_key": None},
            requested_start,
            requested_end,
            info,
        )
    if refresh_history:
        ranges = [(requested_start, requested_end)]
        mode = "refresh"
    elif cache is None:
        ranges = [(requested_start, requested_end)]
        mode = "fill"
    else:
        ranges = _history_missing_date_ranges(requested_start, requested_end, cache.get("daily", []))
        mode = "fill_missing"
    info = {
        "path": history_cache_relative_path(path),
        "status": "HIT" if cache is not None and not ranges else "MISS",
        "identity_mismatches": load_info.get("identity_mismatches", []),
        "requested_start_date": requested_start,
        "requested_end_date": requested_end,
        "missing_date_count_before_fetch": sum(
            len(_history_date_list(start, end)) for start, end in ranges
        ),
        "fetched_ranges": [],
        "api_requests": 0,
        "refresh_requested": refresh_history,
    }
    if cache is not None and not ranges:
        info["status"] = "HIT"
        return _history_cache_record(config, point, year, cache, requested_start, requested_end, info)

    for fetch_start, fetch_end in ranges:
        info["api_requests"] += 1
        record = fetch_point(
            client,
            point=point,
            source="Open-Meteo",
            endpoint=OPEN_METEO_ENDPOINTS["history"],
            model=HISTORY_MODEL,
            params=base_weather_params(
                point,
                models=HISTORY_MODEL_PARAMETER,
                start_date=fetch_start,
                end_date=fetch_end,
            ),
            variables=HISTORY_VARIABLES,
            required_variables=HISTORY_REQUIRED_VARIABLES,
            grid_limit_km=HISTORY_GRID_QA_LIMIT_KM,
            log_label=f"{log_label} {fetch_start}/{fetch_end}",
        )
        if record.get("status") != "PASS":
            info["status"] = "FAILED"
            info["error"] = (record.get("error") or {}).get("reason", "OPEN_METEO_REQUEST_FAILED")
            if cache is not None:
                return _mark_history_cache_record_invalid(
                    _history_cache_record(config, point, year, cache, requested_start, requested_end, info),
                    info,
                    "HISTORY_CACHE_MISSING_DATES_AFTER_FETCH_FAILURE",
                )
            return _mark_history_cache_record_invalid(record, info, "OPEN_METEO_REQUEST_FAILED")
        new_daily = history_cache_daily(record)
        new_identity = history_cache_identity(config, point, year, record)
        candidate_cache = {
            "cache_key": history_cache_key(new_identity),
            "identity": new_identity,
            "record_metadata": {"qa": record.get("qa") or {}},
            "daily": new_daily,
        }
        candidate_mismatches = _history_cache_identity_mismatches(
            candidate_cache,
            config,
            point,
            year,
        )
        if candidate_mismatches:
            info["status"] = "INVALID"
            info["identity_mismatches"] = candidate_mismatches
            return _mark_history_cache_record_invalid(record, info, "HISTORY_CACHE_IDENTITY_INVALID")
        if cache is not None:
            old_identity = cache.get("identity") or {}
            identity_keys = (
                "namespace", "year", "point_id", "source", "endpoint", "model",
                "model_parameter", "requested_coordinate", "returned_grid_coordinate",
                "returned_elevation", "grid_distance_km", "grid_distance_limit_km",
                "grid_cell_key", "cell_selection", "elevation", "timezone",
                "utc_offset_seconds", "solar_variable",
            )
            mismatches = [key for key in identity_keys if old_identity.get(key) != new_identity.get(key)]
            if mismatches:
                info["status"] = "INVALID"
                info["identity_mismatches"] = mismatches
                return _mark_history_cache_record_invalid(record, info, "HISTORY_CACHE_IDENTITY_MISMATCH")
        if cache is None or refresh_history and original_cache is None:
            cache = history_cache_from_record(
                config,
                point,
                year,
                record,
                requested_start,
                requested_end,
                mode=mode,
            )
        else:
            cache.setdefault("retrievals", []).append({
                "retrieved_at": (record.get("response") or {}).get("retrieval_time") or iso_utc(dt.datetime.now(UTC)),
                "requested_start_date": fetch_start,
                "requested_end_date": fetch_end,
                "mode": mode,
                "status": "PASS",
            })
            cache["last_retrieval_time"] = cache["retrievals"][-1]["retrieved_at"]
        if cache is not None:
            _history_cache_merge_daily(cache, new_daily)
            cache["last_request"] = {
                "start_date": fetch_start,
                "end_date": fetch_end,
                "mode": mode,
            }
            write_json(path, cache)
        info["fetched_ranges"].append({"start_date": fetch_start, "end_date": fetch_end})

    info["status"] = "REFRESHED" if refresh_history else "FILLED"
    return _history_cache_record(config, point, year, cache, requested_start, requested_end, info)


def history_cache_stats() -> dict:
    return {
        "cache_hits": 0,
        "cache_misses": 0,
        "cache_fills": 0,
        "cache_refreshes": 0,
        "cache_invalid": 0,
        "cache_failed": 0,
        "api_requests": 0,
        "missing_dates_requested": 0,
    }


def update_history_cache_stats(stats: dict, record: dict) -> None:
    info = record.get("history_cache") or {}
    status = info.get("status")
    if status == "HIT":
        stats["cache_hits"] += 1
    elif status == "MISS":
        stats["cache_misses"] += 1
    elif status == "FILLED":
        stats["cache_fills"] += 1
    elif status == "REFRESHED":
        stats["cache_refreshes"] += 1
    elif status == "INVALID":
        stats["cache_invalid"] += 1
    elif status == "FAILED":
        stats["cache_failed"] += 1
    stats["api_requests"] += int(info.get("api_requests", 0) or 0)
    stats["missing_dates_requested"] += int(info.get("missing_date_count_before_fetch", 0) or 0)


# ---------------------------------------------------------------------------
# Derived weather-event cache and transparent weather heuristics
# ---------------------------------------------------------------------------

WEATHER_EVENT_SOURCE_KEYS = (
    "date",
    "complete",
    "temperature_min_c",
    "temperature_max_c",
    "temperature_mean_c",
    "night_min_c",
    "precipitation_mm",
    "snowfall_cm",
    "wind_speed_mean_kmh",
    "wind_gust_max_kmh",
)
WEATHER_EVENT_FLAG_KEYS = (
    "freeze",
    "hard_freeze_le_minus5",
    "gust_ge_50",
    "gust_ge_65",
    "rain_day",
    "snow_day",
    "rain_and_gust_ge_50",
    "snow_and_gust_ge_50",
    "freeze_and_snow",
)


def weather_events_cache_path(
    config: dict,
    year: int,
    point_id: str,
    cache_dir: Path | None = None,
) -> Path:
    root = Path(cache_dir) if cache_dir is not None else WEATHER_EVENTS_CACHE_DIR
    return root / history_cache_namespace(config) / str(year) / f"{point_id}.json"


def weather_event_source_fingerprint(day: dict) -> str:
    """Hash only the source daily values used by the derived event layer."""
    values = {key: day.get(key) for key in WEATHER_EVENT_SOURCE_KEYS}
    encoded = json.dumps(values, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _weather_event_flag(value: float | None, predicate) -> bool | None:
    return predicate(value) if value is not None else None


def mechanical_leaf_stress(day: dict) -> dict:
    """Classify weather mechanical pressure; this is not a leaf-loss probability."""
    gust = metric_value(day, "wind_gust_max_kmh")
    rain = day.get("rain_day")
    snow = day.get("snow_day")
    freeze = day.get("freeze")
    hard_freeze = day.get("hard_freeze_le_minus5")
    reasons = []
    if gust is not None and gust >= 65:
        reasons.append("very_strong_wind")
    elif gust is not None and gust >= 50:
        reasons.append("strong_wind")
    if rain is True and gust is not None and gust >= 50:
        reasons.append("wind_plus_rain")
    if snow is True and gust is not None and gust >= 50:
        reasons.append("wind_plus_snow")
    if hard_freeze is True and snow is True:
        reasons.append("hard_freeze_plus_snow")
    elif freeze is True and snow is True:
        reasons.append("freeze_plus_snow")
    if gust is not None and gust >= 65 or (hard_freeze is True and snow is True) or (snow is True and gust is not None and gust >= 50):
        level = "HIGH"
    elif (gust is not None and gust >= 50) or rain is True or snow is True or freeze is True:
        level = "MEDIUM"
    elif any(value is not None for value in (gust, rain, snow, freeze)):
        level = "LOW"
    else:
        level = "UNDETERMINED"
    return {
        "level": level,
        "reasons": reasons,
        "rule_version": MECHANICAL_LEAF_STRESS_RULE_VERSION,
    }


def derive_weather_event_day(day: dict, source_state: str) -> dict:
    """Convert one complete source daily row into a weather-only event row."""
    if source_state not in {"finalized_history", "forecast"}:
        raise ValueError(f"unsupported weather event source_state: {source_state}")
    minimum = metric_value(day, "temperature_min_c")
    maximum = metric_value(day, "temperature_max_c")
    precipitation = metric_value(day, "precipitation_mm")
    snowfall = metric_value(day, "snowfall_cm")
    gust = metric_value(day, "wind_gust_max_kmh")
    freeze = _weather_event_flag(minimum, lambda value: value < 0)
    hard_freeze = _weather_event_flag(minimum, lambda value: value <= -5)
    gust_50 = _weather_event_flag(gust, lambda value: value >= 50)
    gust_65 = _weather_event_flag(gust, lambda value: value >= 65)
    rain = _weather_event_flag(precipitation, lambda value: value > 0)
    snow = _weather_event_flag(snowfall, lambda value: value > 0)
    event = {
        "date": day.get("date"),
        "source_state": source_state,
        "complete": bool(day.get("complete")),
        "dtr_c": round(maximum - minimum, 3) if minimum is not None and maximum is not None else None,
        "temperature_mean_c": metric_value(day, "temperature_mean_c"),
        "temperature_min_c": minimum,
        "temperature_max_c": maximum,
        "night_min_c": metric_value(day, "night_min_c"),
        "precipitation_mm": precipitation,
        "snowfall_cm": snowfall,
        "wind_speed_mean_kmh": metric_value(day, "wind_speed_mean_kmh"),
        "wind_gust_max_kmh": gust,
        "freeze": freeze,
        "hard_freeze_le_minus5": hard_freeze,
        "gust_ge_50": gust_50,
        "gust_ge_65": gust_65,
        "rain_day": rain,
        "snow_day": snow,
        "rain_and_gust_ge_50": True if rain is True and gust_50 is True else False if rain is not None and gust_50 is not None else None,
        "snow_and_gust_ge_50": True if snow is True and gust_50 is True else False if snow is not None and gust_50 is not None else None,
        "freeze_and_snow": True if freeze is True and snow is True else False if freeze is not None and snow is not None else None,
        "source_fingerprint": weather_event_source_fingerprint(day),
    }
    event["mechanical_leaf_stress"] = mechanical_leaf_stress(event)
    return event


def weather_events_cache_identity(config: dict, point: dict, year: int, history_cache: dict) -> dict:
    source_identity = copy.deepcopy(history_cache.get("identity") or {})
    return {
        "namespace": history_cache_namespace(config),
        "year": int(year),
        "point_id": point.get("id"),
        "source": source_identity.get("source", "Open-Meteo"),
        "endpoint": source_identity.get("endpoint", OPEN_METEO_ENDPOINTS["history"]),
        "model": source_identity.get("model", HISTORY_MODEL),
        "model_parameter": source_identity.get("model_parameter", HISTORY_MODEL_PARAMETER),
        "requested_coordinate": copy.deepcopy(source_identity.get("requested_coordinate")),
        "returned_grid_coordinate": copy.deepcopy(source_identity.get("returned_grid_coordinate")),
        "returned_elevation": source_identity.get("returned_elevation"),
        "grid_distance_km": source_identity.get("grid_distance_km"),
        "grid_distance_limit_km": source_identity.get("grid_distance_limit_km", HISTORY_GRID_QA_LIMIT_KM),
        "grid_cell_key": source_identity.get("grid_cell_key"),
        "cell_selection": source_identity.get("cell_selection"),
        "elevation": source_identity.get("elevation"),
        "timezone": source_identity.get("timezone"),
        "utc_offset_seconds": source_identity.get("utc_offset_seconds"),
        "source_history_cache_key": history_cache.get("cache_key"),
    }


def _weather_events_cache_identity_mismatches(
    cache: dict,
    config: dict,
    point: dict,
    year: int,
    history_cache: dict,
) -> list[str]:
    expected = weather_events_cache_identity(config, point, year, history_cache)
    actual = cache.get("identity") if isinstance(cache.get("identity"), dict) else {}
    mismatches = [
        key for key, value in expected.items()
        if actual.get(key) != value
    ]
    if cache.get("schema_version") not in COMPATIBLE_SCHEMA_VERSIONS:
        mismatches.append("schema_version")
    if cache.get("cache_schema_version") != WEATHER_EVENTS_CACHE_SCHEMA_VERSION:
        mismatches.append("cache_schema_version")
    if cache.get("cache_kind") != "derived_weather_events":
        mismatches.append("cache_kind")
    if cache.get("namespace") != history_cache_namespace(config):
        mismatches.append("namespace")
    try:
        cache_year = int(cache.get("year", -1))
    except (TypeError, ValueError):
        cache_year = -1
    if cache_year != int(year):
        mismatches.append("year")
    if cache.get("point_id") != point.get("id"):
        mismatches.append("point_id")
    daily = cache.get("daily")
    if not isinstance(daily, list):
        mismatches.append("daily")
    else:
        dates = [item.get("date") for item in daily if isinstance(item, dict)]
        if any(not isinstance(value, str) for value in dates):
            mismatches.append("daily_date")
        if len(dates) != len(set(dates)):
            mismatches.append("duplicate_daily_date")
    fingerprints = cache.get("source_fingerprints_by_date")
    if not isinstance(fingerprints, dict):
        mismatches.append("source_fingerprints_by_date")
    return sorted(set(mismatches))


def load_weather_events_cache(
    config: dict,
    point: dict,
    year: int,
    history_cache: dict,
    cache_dir: Path | None = None,
) -> tuple[dict | None, dict]:
    path = weather_events_cache_path(config, year, point["id"], cache_dir)
    info = {"path": history_cache_relative_path(path), "status": "MISS", "identity_mismatches": []}
    if not path.is_file():
        return None, info
    try:
        with path.open(encoding="utf-8") as handle:
            cache = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        info.update({"status": "INVALID", "identity_mismatches": [f"CACHE_READ_FAILED:{type(error).__name__}"]})
        return None, info
    mismatches = _weather_events_cache_identity_mismatches(cache, config, point, year, history_cache)
    if mismatches:
        info.update({"status": "INVALID", "identity_mismatches": mismatches})
        return None, info
    info.update({
        "status": "HIT",
        "cached_dates": len(cache.get("daily") or []),
        "cache_key": cache.get("source_history_cache_key"),
    })
    return cache, info


def weather_event_cache_stats() -> dict:
    return {
        "cache_hits": 0,
        "cache_fills": 0,
        "cache_invalid": 0,
        "dates_added": 0,
        "dates_recomputed": 0,
        "source_dates_changed": 0,
        "dates_removed": 0,
        "files_written": 0,
    }


def update_weather_event_cache_stats(total: dict, item: dict) -> None:
    stats = item.get("cache_update") or {}
    for key in total:
        total[key] += int(stats.get(key, 0) or 0)


def _weather_events_cache_record(
    config: dict,
    point: dict,
    year: int,
    history_cache: dict,
    daily: list[dict],
    generated_at: str,
    stats: dict,
) -> dict:
    identity = weather_events_cache_identity(config, point, year, history_cache)
    fingerprints = {
        item["date"]: item["source_fingerprint"]
        for item in daily
        if item.get("date") and item.get("source_fingerprint")
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "cache_schema_version": WEATHER_EVENTS_CACHE_SCHEMA_VERSION,
        "cache_kind": "derived_weather_events",
        "namespace": history_cache_namespace(config),
        "year": int(year),
        "point_id": point.get("id"),
        "source_history_cache_key": history_cache.get("cache_key"),
        "source_history_cache_path": history_cache.get("_cache_path"),
        "identity": identity,
        "daily": daily,
        "cached_dates": sorted(fingerprints),
        "source_fingerprints_by_date": fingerprints,
        "date_range": {
            "start_date": min(fingerprints) if fingerprints else None,
            "end_date": max(fingerprints) if fingerprints else None,
        },
        "rule_versions": {
            "weather_event": WEATHER_EVENT_RULE_VERSION,
            "cooling_episode": COOLING_EPISODE_RULE_VERSION,
            "mechanical_leaf_stress": MECHANICAL_LEAF_STRESS_RULE_VERSION,
        },
        "cache_update": copy.deepcopy(stats),
        "last_update": generated_at,
    }


def update_weather_events_cache(
    config: dict,
    point: dict,
    year: int,
    history_cache: dict,
    generated_at: str,
    *,
    cache_dir: Path | None = None,
) -> tuple[dict | None, dict]:
    """Incrementally derive events from one validated Historical daily cache."""
    source_days = [
        copy.deepcopy(day)
        for day in history_cache.get("daily", [])
        if isinstance(day, dict)
        and day.get("complete")
        and isinstance(day.get("date"), str)
        and day["date"].startswith(f"{int(year)}-")
    ]
    source_days.sort(key=lambda item: item["date"])
    history_cache = copy.deepcopy(history_cache)
    cache, load_info = load_weather_events_cache(config, point, year, history_cache, cache_dir)
    stats = weather_event_cache_stats()
    if load_info.get("status") == "INVALID":
        stats["cache_invalid"] = 1
        return None, {
            "status": "INVALID",
            "identity_mismatches": load_info.get("identity_mismatches", []),
            "path": load_info.get("path"),
            "cache_update": stats,
        }
    incoming_fingerprints = {
        day["date"]: weather_event_source_fingerprint(day)
        for day in source_days
    }
    old_fingerprints = (cache or {}).get("source_fingerprints_by_date") or {}
    old_days = {
        day.get("date"): copy.deepcopy(day)
        for day in (cache or {}).get("daily", [])
        if isinstance(day, dict) and isinstance(day.get("date"), str)
    }
    added_dates = sorted(set(incoming_fingerprints) - set(old_fingerprints))
    changed_dates = sorted(
        date_value
        for date_value in set(incoming_fingerprints) & set(old_fingerprints)
        if incoming_fingerprints[date_value] != old_fingerprints[date_value]
    )
    removed_dates = sorted(set(old_fingerprints) - set(incoming_fingerprints))
    stats["dates_added"] = len(added_dates)
    stats["dates_recomputed"] = len(changed_dates)
    stats["source_dates_changed"] = len(changed_dates)
    stats["dates_removed"] = len(removed_dates)
    if cache is not None and not added_dates and not changed_dates and not removed_dates:
        stats["cache_hits"] = 1
        return cache, {
            "status": "HIT",
            "path": load_info.get("path"),
            "cache_update": stats,
        }
    if cache is None:
        stats["cache_fills"] = 1
    recompute_dates = set(added_dates) | set(changed_dates)
    for day in source_days:
        if day["date"] in recompute_dates:
            old_days[day["date"]] = derive_weather_event_day(day, "finalized_history")
    for date_value in removed_dates:
        old_days.pop(date_value, None)
    derived_days = [old_days[key] for key in sorted(old_days)]
    stats["files_written"] = 1
    cache = _weather_events_cache_record(config, point, year, history_cache, derived_days, generated_at, stats)
    path = weather_events_cache_path(config, year, point["id"], cache_dir)
    write_json(path, cache)
    cache["cache_update"] = copy.deepcopy(stats)
    return cache, {
        "status": "FILLED" if stats["cache_fills"] else "UPDATED",
        "path": history_cache_relative_path(path),
        "cache_update": stats,
    }


def run_history(
    config: dict,
    client: ApiClient,
    generated_at: str,
    data_date: str,
    completed_date: dt.date,
    *,
    refresh_history: bool = False,
    forward_anchor_date: dt.date | None = None,
    cache_dir: Path | None = None,
) -> dict:
    points = active_points(config)
    configured_years = history_years_for_config(config)
    point_results: dict[str, dict] = {}
    all_records = []
    cache_stats = history_cache_stats()
    for point_id, point in points.items():
        region_config = config.get("regions", {}).get(point["region"], {})
        history_start = region_config.get(
            "history_start_month_day",
            config.get("history_start_month_day", "08-25"),
        )
        years: dict[str, dict] = {}
        for year in configured_years:
            date_range = history_date_range(completed_date, year, history_start)
            cache_range = history_cache_required_range(
                config,
                point,
                year,
                completed_date,
                forward_anchor_date,
            )
            if date_range is None:
                if cache_range is None:
                    record = invalid_record(
                        point=point,
                        source="Open-Meteo",
                        endpoint=OPEN_METEO_ENDPOINTS["history"],
                        model=HISTORY_MODEL,
                        request_params={"models": HISTORY_MODEL_PARAMETER, "year": year},
                        reason="HISTORY_NOT_STARTED",
                    )
                else:
                    record = history_cache_record_or_fetch(
                        config,
                        client,
                        point,
                        year,
                        cache_range[0],
                        cache_range[1],
                        refresh_history=refresh_history,
                        cache_dir=cache_dir,
                        log_label=f"{point_id}:HISTORY {year}",
                    )
                    record["status"] = "INVALID"
                    record.setdefault("qa", {})["valid"] = False
                    record["qa"]["final_status"] = "INVALID"
                    record["qa"]["reason"] = "HISTORY_NOT_STARTED"
                    record["daily"] = []
            else:
                start_date, end_date = date_range
                cache_range = cache_range or (start_date, end_date)
                record = history_cache_record_or_fetch(
                    config,
                    client,
                    point,
                    year,
                    cache_range[0],
                    cache_range[1],
                    refresh_history=refresh_history,
                    cache_dir=cache_dir,
                    log_label=f"{point_id}:HISTORY {year}",
                )
                if record.get("status") == "PASS":
                    log(f"[{point_id}] HISTORY {year} OK")
                if record.get("daily") and date_range != cache_range:
                    logical_start, logical_end = date_range
                    record["daily"] = [
                        day for day in record["daily"]
                        if logical_start <= day.get("date", "") <= logical_end
                    ]
                    record.setdefault("history_cache", {})["logical_requested_range"] = {
                        "start_date": logical_start,
                        "end_date": logical_end,
                    }
            years[str(year)] = record
            all_records.append(record)
            update_history_cache_stats(cache_stats, record)
        point_results[point_id] = {
            "point_id": point_id,
            "point": {"name": point["name"], "region": point["region"], "status": point["status"]},
            "history_start_month_day": history_start,
            "years": years,
        }
    comparison = build_history_comparison(config, point_results, data_date)
    status = "OK" if all(record.get("status") == "PASS" for record in all_records) and all_records else "FAILED"
    return module_header(
        "history",
        generated_at,
        data_date,
        status,
        endpoint=OPEN_METEO_ENDPOINTS["history"],
        model=HISTORY_MODEL,
        model_parameter=HISTORY_MODEL_PARAMETER,
        history_years=list(configured_years),
        period_start=" / ".join(
            f"{year}-{config.get('history_start_month_day', '08-25')}"
            for year in configured_years
        ),
        period_end=data_date,
        note="Historical IFS is reanalysis/analysis, not station observation.",
        points=point_results,
        region_summaries=comparison,
        excluded_points=excluded_points(config),
        successful_fetches=sum(record.get("status") == "PASS" for record in all_records),
        failed_fetches=sum(record.get("status") != "PASS" for record in all_records),
        history_cache={
            "enabled": True,
            "directory": history_cache_relative_path(Path(cache_dir) if cache_dir is not None else HISTORY_CACHE_DIR),
            "refresh_requested": refresh_history,
            **cache_stats,
        },
    )


def history_forward_window_applicable(definition: dict | None) -> bool:
    """True when a window definition still overlaps the hard cutoff date.

    A window whose start is past the cutoff has zero days by construction.  It
    carries no data and must never be judged as a missing-data failure.
    """
    if not isinstance(definition, dict):
        return False
    return definition.get("status") != HISTORY_FORWARD_WINDOW_NOT_APPLICABLE


def history_forward_applicable_window_keys(definitions) -> list[str]:
    """Return the window keys that are still inside the cutoff date."""
    if isinstance(definitions, dict):
        items = list(definitions.items())
    else:
        items = [(item.get("window"), item) for item in (definitions or [])]
    return [key for key, definition in items if history_forward_window_applicable(definition)]


def history_forward_window_definitions(
    forecast_date: dt.date,
    cutoff_month_day: str = HISTORY_FORWARD_CUTOFF_MONTH_DAY,
) -> list[dict]:
    """Build rolling same-calendar-date windows, clipped at the hard cutoff."""
    try:
        cutoff_month, cutoff_day = (int(value) for value in cutoff_month_day.split("-", 1))
        cutoff_date = dt.date(forecast_date.year, cutoff_month, cutoff_day)
    except (TypeError, ValueError) as error:
        raise ValueError(f"invalid history forward cutoff month-day: {cutoff_month_day}") from error
    raw_windows = (
        ("d0_7", 0, 7),
        ("d8_15", 8, 15),
        ("d16_to_11_01", 16, None),
    )
    definitions = []
    for key, start_offset, end_offset in raw_windows:
        requested_start = forecast_date + dt.timedelta(days=start_offset)
        requested_end = cutoff_date if end_offset is None else forecast_date + dt.timedelta(days=end_offset)
        if requested_start > cutoff_date:
            definitions.append({
                "window": key,
                "offset_start_days": start_offset,
                "offset_end_days": end_offset,
                "requested_start_date": None,
                "requested_end_date": None,
                "start_date": None,
                "end_date": None,
                "cutoff_date": cutoff_date.isoformat(),
                "status": HISTORY_FORWARD_WINDOW_NOT_APPLICABLE,
                "reason": "WINDOW_AFTER_CUTOFF",
            })
            continue
        definitions.append({
            "window": key,
            "offset_start_days": start_offset,
            "offset_end_days": end_offset,
            "requested_start_date": requested_start.isoformat(),
            "requested_end_date": min(requested_end, cutoff_date).isoformat(),
            "start_date": requested_start.isoformat(),
            "end_date": min(requested_end, cutoff_date).isoformat(),
            "cutoff_date": cutoff_date.isoformat(),
            "status": "OK",
            "reason": None,
        })
    return definitions


def history_forward_windows_for_year(forecast_date: dt.date, year: int) -> dict[str, dict]:
    """Translate the current calendar windows to one historical calendar year."""
    windows = {}
    for definition in history_forward_window_definitions(forecast_date):
        translated = copy.deepcopy(definition)
        for field in ("requested_start_date", "requested_end_date", "start_date", "end_date", "cutoff_date"):
            value = translated.get(field)
            if value:
                source_date = dt.date.fromisoformat(value)
                translated[field] = dt.date(year, source_date.month, source_date.day).isoformat()
        windows[translated["window"]] = translated
    return windows


def _history_forward_window_unavailable(
    definition: dict,
    reason: str,
    status: str = "UNAVAILABLE",
) -> dict:
    return {
        "status": status,
        "usable_for_cross_year_comparison": False,
        "start_date": definition.get("start_date"),
        "end_date": definition.get("end_date"),
        "expected_days": 0,
        "days_available": 0,
        "missing_dates": [],
        "incomplete_dates": [],
        "daily": [],
        "metrics": None,
        "reason": reason,
    }


def _definition_window_status(definition: dict, *, default: str = "UNAVAILABLE") -> str:
    """Mirror a structurally empty window definition instead of faking failure."""
    status = (definition or {}).get("status")
    if status in {"UNAVAILABLE", HISTORY_FORWARD_WINDOW_NOT_APPLICABLE}:
        return status
    return default


def history_forward_window_summary(days: list[dict], definition: dict) -> dict:
    """Summarize one historical window without inferring missing days."""
    if definition.get("status") != "OK" or not definition.get("start_date") or not definition.get("end_date"):
        return _history_forward_window_unavailable(
            definition,
            definition.get("reason") or "WINDOW_UNAVAILABLE",
            status=_definition_window_status(definition),
        )
    start_date = dt.date.fromisoformat(definition["start_date"])
    end_date = dt.date.fromisoformat(definition["end_date"])
    expected_dates = []
    cursor = start_date
    while cursor <= end_date:
        expected_dates.append(cursor.isoformat())
        cursor += dt.timedelta(days=1)
    selected = [
        day for day in days
        if isinstance(day.get("date"), str)
        and start_date <= dt.date.fromisoformat(day["date"]) <= end_date
    ]
    available_dates = {day["date"] for day in selected}
    incomplete_dates = sorted(day["date"] for day in selected if not day.get("complete"))
    missing_dates = [value for value in expected_dates if value not in available_dates]
    complete_days = [day for day in selected if day.get("complete")]
    complete = not missing_dates and not incomplete_dates and len(complete_days) == len(expected_dates)
    metrics = period_metrics(selected)
    daily_means = [metric_value(day, "temperature_mean_c") for day in complete_days]
    daily_means = [value for value in daily_means if value is not None]
    daily_mins = [metric_value(day, "temperature_min_c") for day in complete_days]
    daily_mins = [value for value in daily_mins if value is not None]
    daily_maxs = [metric_value(day, "temperature_max_c") for day in complete_days]
    daily_maxs = [value for value in daily_maxs if value is not None]
    solar_values = [
        float(day["solar_metric"]["value"])
        for day in complete_days
        if isinstance(day.get("solar_metric"), dict)
        and isinstance(day["solar_metric"].get("value"), (int, float))
    ]
    solar_variable = next(
        (
            day["solar_metric"].get("variable")
            for day in complete_days
            if isinstance(day.get("solar_metric"), dict) and day["solar_metric"].get("variable")
        ),
        None,
    )
    solar_unit = next(
        (
            day["solar_metric"].get("unit")
            for day in complete_days
            if isinstance(day.get("solar_metric"), dict) and day["solar_metric"].get("unit")
        ),
        None,
    )
    first_three = daily_means[:3] if len(daily_means) >= 6 else []
    last_three = daily_means[-3:] if len(daily_means) >= 6 else []
    first_mean = safe_mean(first_three)
    last_mean = safe_mean(last_three)
    trend_delta = round(last_mean - first_mean, 3) if first_mean is not None and last_mean is not None else None
    if trend_delta is None:
        trend_direction = "UNDETERMINED"
    elif trend_delta >= 0.5:
        trend_direction = "WARMING"
    elif trend_delta <= -0.5:
        trend_direction = "COOLING"
    else:
        trend_direction = "NEAR_FLAT"
    metrics.update({
        "average_temperature_c": safe_mean(daily_means),
        "minimum_temperature_c": round(min(daily_mins), 3) if daily_mins else None,
        "maximum_temperature_c": round(max(daily_maxs), 3) if daily_maxs else None,
        "total_precipitation_mm": metrics.get("precipitation_mm"),
        "total_snowfall_cm": metrics.get("snowfall_cm"),
        "average_daily_solar_value": safe_mean(solar_values),
        "average_daily_solar_variable": solar_variable,
        "average_daily_solar_unit": solar_unit,
        "average_daily_sunshine_duration_seconds": (
            safe_mean(solar_values) if solar_variable == "sunshine_duration" else None
        ),
        "maximum_wind_gust_kmh": metrics.get("wind_gust_max_kmh"),
        "temperature_trend": {
            "first_3_days_mean_temperature_c": first_mean,
            "last_3_days_mean_temperature_c": last_mean,
            "last_3_minus_first_3_mean_temperature_c": trend_delta,
            "direction": trend_direction,
            "method": "last_3_complete_daily_means_minus_first_3_complete_daily_means; threshold=0.5C",
        },
    })
    return {
        "status": "OK" if complete else "INVALID",
        "usable_for_cross_year_comparison": complete,
        "start_date": definition["start_date"],
        "end_date": definition["end_date"],
        "expected_days": len(expected_dates),
        "days_available": len(complete_days),
        "missing_dates": missing_dates,
        "incomplete_dates": incomplete_dates,
        "daily": selected,
        "metrics": metrics,
        "reason": None if complete else "HISTORY_FORWARD_WINDOW_INCOMPLETE",
    }


def history_forward_same_grid_qa(years: dict[str, dict]) -> dict:
    """Apply the existing historical grid rule to the three forward-reference years."""
    result = historical_same_grid_qa(years, HISTORY_FORWARD_YEARS)
    year_qa = {
        str(year): {
            "record_status": (years.get(str(year)) or {}).get("status", "INVALID"),
            "final_status": ((years.get(str(year)) or {}).get("qa") or {}).get("final_status", "INVALID"),
            "grid_distance_km": ((years.get(str(year)) or {}).get("qa") or {}).get("grid_distance_km"),
            "grid_distance_limit_km": ((years.get(str(year)) or {}).get("qa") or {}).get(
                "grid_distance_limit_km", HISTORY_GRID_QA_LIMIT_KM
            ),
            "distance_check": (
                "PASS"
                if isinstance(((years.get(str(year)) or {}).get("qa") or {}).get("grid_distance_km"), (int, float))
                and ((years.get(str(year)) or {}).get("qa") or {}).get("grid_distance_km") <= HISTORY_GRID_QA_LIMIT_KM
                else "FAIL"
            ),
        }
        for year in HISTORY_FORWARD_YEARS
    }
    result["grid_distance_limit_km"] = HISTORY_GRID_QA_LIMIT_KM
    result["year_qa"] = year_qa
    failed_years = [
        year for year, item in year_qa.items()
        if item["record_status"] != "PASS" or item["final_status"] != "PASS" or item["distance_check"] != "PASS"
    ]
    if failed_years:
        result["status"] = "FAIL"
        result["final_status"] = "FAILED"
        result["reason"] = "HISTORY_FORWARD_YEAR_QA_FAILED:" + ",".join(failed_years)
    result["cross_year_comparison_usable"] = result["final_status"] == "PASS"
    return result


SIGUNIANG_SUBREGION_KEYS = ("shuangqiao", "bipenggou")
JIUZHAIGOU_SUBREGION_KEYS = ("shuzheng", "rize", "zezhawa")
SUBREGION_KEYS_BY_REGION = {
    "siguniang": SIGUNIANG_SUBREGION_KEYS,
    "jiuzhaigou": JIUZHAIGOU_SUBREGION_KEYS,
}
SUBREGION_CONFIG_KEYS = {
    "siguniang": "siguniang_subregions",
    "jiuzhaigou": "jiuzhaigou_subregions",
}
LIGHTWEIGHT_WINDOW_METRIC_KEYS = (
    "temperature_mean_c",
    "temperature_max_mean_c",
    "night_min_mean_c",
    "absolute_min_night_c",
    "nights_below_15c",
    "nights_below_10c",
    "nights_below_5c",
    "nights_below_2c",
    "nights_below_0c",
    "diurnal_temperature_range_mean_c",
    "precipitation_total_mm",
    "snowfall_total_cm",
    "average_daily_sunshine_duration_seconds",
    "average_daily_shortwave_radiation_w_m2",
    "max_wind_gust_kmh",
    "strong_wind_day_count",
)


def validate_subregion_config(
    config: dict,
    region_id: str,
    subregion_keys: tuple[str, ...],
    config_key: str,
) -> None:
    configured = config.get(config_key)
    if not isinstance(configured, dict):
        return
    seen = set()
    for subregion_id in subregion_keys:
        item = configured.get(subregion_id)
        if not isinstance(item, dict) or not isinstance(item.get("point_ids"), list) or not item.get("point_ids"):
            raise ValueError(f"{region_id} subregion registry missing point_ids: {subregion_id}")
        for point_id in item["point_ids"]:
            if point_id in seen:
                raise ValueError(f"{region_id} point appears in multiple subregions: {point_id}")
            seen.add(point_id)
            point = config.get("points", {}).get(point_id)
            if not isinstance(point, dict) or point.get("region") != region_id:
                raise ValueError(f"{region_id} subregion point is not a {region_id} point: {point_id}")
            if point.get("subregion") != subregion_id:
                raise ValueError(f"{region_id} point subregion mismatch: {point_id}")


def validate_siguniang_subregion_config(config: dict) -> None:
    validate_subregion_config(config, "siguniang", SIGUNIANG_SUBREGION_KEYS, "siguniang_subregions")


def validate_jiuzhaigou_subregion_config(config: dict) -> None:
    validate_subregion_config(config, "jiuzhaigou", JIUZHAIGOU_SUBREGION_KEYS, "jiuzhaigou_subregions")


def region_subregion_registry(config: dict, region_id: str) -> dict[str, dict]:
    """Return the explicit spatial registry for a registered region."""
    subregion_keys = SUBREGION_KEYS_BY_REGION.get(region_id, ())
    config_key = SUBREGION_CONFIG_KEYS.get(region_id)
    configured = config.get(config_key) if config_key else None
    if not isinstance(configured, dict) or not configured:
        # Unlike the upstream template there is no built-in fallback registry:
        # the configured subregion blocks are the only source of truth, and
        # validate_subregion_config() already refuses to run without them.
        return {}
    result = {}
    for subregion_id in subregion_keys:
        item = configured.get(subregion_id)
        if not isinstance(item, dict):
            continue
        point_ids = item.get("point_ids")
        if not isinstance(point_ids, list):
            continue
        result[subregion_id] = {
            "name": item.get("name", subregion_id),
            "point_ids": list(dict.fromkeys(str(point_id) for point_id in point_ids)),
            "minimum_verified_unique_grids": max(1, int(item.get("minimum_verified_unique_grids", 1))),
        }
    return result


def siguniang_subregion_registry(config: dict) -> dict[str, dict]:
    """Return the explicit Siguniang subregion registry with a safe v1 fallback."""
    return region_subregion_registry(config, "siguniang")


def jiuzhaigou_subregion_registry(config: dict) -> dict[str, dict]:
    return region_subregion_registry(config, "jiuzhaigou")


def region_subregion_point_ids(
    config: dict,
    region_id: str,
    subregion_id: str,
    *,
    verified_only: bool = False,
) -> list[str]:
    registry = region_subregion_registry(config, region_id)
    item = registry.get(subregion_id) or {}
    result = []
    for point_id in item.get("point_ids", []):
        point = config.get("points", {}).get(point_id)
        if not isinstance(point, dict) or point.get("region") != region_id:
            continue
        if verified_only and point.get("status") != "VERIFIED":
            continue
        result.append(point_id)
    return result


def siguniang_subregion_point_ids(
    config: dict,
    subregion_id: str,
    *,
    verified_only: bool = False,
) -> list[str]:
    return region_subregion_point_ids(config, "siguniang", subregion_id, verified_only=verified_only)


def jiuzhaigou_subregion_point_ids(
    config: dict,
    subregion_id: str,
    *,
    verified_only: bool = False,
) -> list[str]:
    return region_subregion_point_ids(config, "jiuzhaigou", subregion_id, verified_only=verified_only)


def history_forward_point_ids(config: dict) -> list[str]:
    """Return only points that the forward-history module is allowed to query."""
    point_ids = []
    points = active_points(config)
    for region_id, region_config in config.get("regions", {}).items():
        if region_id in SUBREGION_KEYS_BY_REGION:
            candidates = [
                point_id
                for subregion_id in SUBREGION_KEYS_BY_REGION[region_id]
                for point_id in region_subregion_point_ids(
                    config,
                    region_id,
                    subregion_id,
                    verified_only=True,
                )
            ]
        else:
            candidates = [region_config.get("core_point_id")]
        for point_id in candidates:
            if point_id in points and point_id not in point_ids:
                point_ids.append(point_id)
    return point_ids


def point_grid_mapping(record: dict, *, point_id: str | None = None, year: int | None = None) -> dict:
    response = record.get("response") or {}
    request = record.get("request") or {}
    qa = record.get("qa") or {}
    mapping = {
        "point_id": point_id or record.get("point_id"),
        "year": year,
        "requested_coordinate": request.get("coordinate"),
        "returned_grid_coordinate": response.get("grid_coordinate"),
        "returned_elevation": response.get("returned_elevation"),
        "grid_distance_km": qa.get("grid_distance_km"),
        "grid_distance_limit_km": qa.get("grid_distance_limit_km"),
        "grid_cell_key": record_grid_cell_key(record),
        "timezone": response.get("timezone"),
        "utc_offset_seconds": response.get("utc_offset_seconds"),
        "source": record.get("source"),
        "endpoint": record.get("endpoint"),
        "model": record.get("model"),
        "status": record.get("status", "INVALID"),
        "qa_final_status": qa.get("final_status", "INVALID"),
    }
    return mapping


def deduplicate_grid_records(records: list[dict]) -> list[dict]:
    """Keep one representative record per returned model grid and retain its mapping."""
    cells: dict[str, dict] = {}
    for record in records:
        if record.get("status") != "PASS" or not record_grid_cell_key(record):
            continue
        mapping = point_grid_mapping(record)
        cell_key = mapping["grid_cell_key"]
        entry = cells.setdefault(
            cell_key,
            {
                "grid_cell_id": cell_key,
                "returned_grid_coordinate": mapping["returned_grid_coordinate"],
                "returned_elevation": mapping["returned_elevation"],
                "representative_point_id": record.get("point_id"),
                "point_ids": [],
                "mappings": [],
                "record": record,
            },
        )
        point_id = record.get("point_id")
        if point_id and point_id not in entry["point_ids"]:
            entry["point_ids"].append(point_id)
        entry["mappings"].append(mapping)
    return list(cells.values())


def grid_sampling_summary(
    config: dict,
    point_ids: list[str],
    point_records: dict[str, dict],
    *,
    minimum_verified_unique_grids: int = 1,
) -> dict:
    """Describe requested points and unique returned grids without weighting duplicates."""
    mappings = []
    valid_records = []
    verified_point_ids = []
    for point_id in point_ids:
        point = config.get("points", {}).get(point_id) or {}
        if point.get("status") == "VERIFIED":
            verified_point_ids.append(point_id)
        record = point_records.get(point_id)
        if record is None:
            mappings.append({
                "point_id": point_id,
                "requested_coordinate": {"latitude": point.get("latitude"), "longitude": point.get("longitude")},
                "status": "EXCLUDED",
                "usable_for_main_chain": False,
                "reason": point.get("reason") or "PROVISIONAL_POINT_EXCLUDED",
            })
            continue
        mapping = point_grid_mapping(record, point_id=point_id)
        mappings.append(mapping)
        if record.get("status") == "PASS" and mapping.get("grid_cell_key") and mapping.get("qa_final_status") in {"PASS", None}:
            valid_records.append(record)
    unique_entries = deduplicate_grid_records(valid_records)
    valid_point_ids = [record.get("point_id") for record in valid_records if record.get("point_id")]
    if not valid_records:
        status = "INVALID"
        reason = "NO_VALID_VERIFIED_GRID"
    elif len(unique_entries) < max(1, minimum_verified_unique_grids):
        status = "PARTIAL"
        reason = "INSUFFICIENT_VERIFIED_UNIQUE_GRIDS"
    else:
        status = "OK"
        reason = None
    return {
        "requested_points": len(point_ids),
        "verified_points": len(verified_point_ids),
        "queried_points": len(point_records),
        "valid_points": len(valid_point_ids),
        "failed_points": max(0, len(point_records) - len(valid_point_ids)),
        "excluded_point_ids": [point_id for point_id in point_ids if point_id not in point_records],
        "valid_point_ids": valid_point_ids,
        "unique_model_grids": len(unique_entries),
        "grid_coordinates": [entry["returned_grid_coordinate"] for entry in unique_entries],
        "grid_cell_ids": [entry["grid_cell_id"] for entry in unique_entries],
        "point_to_grid": mappings,
        "unique_grid_mappings": [
            {
                "grid_cell_id": entry["grid_cell_id"],
                "returned_grid_coordinate": entry["returned_grid_coordinate"],
                "returned_elevation": entry["returned_elevation"],
                "point_ids": entry["point_ids"],
            }
            for entry in unique_entries
        ],
        "minimum_verified_unique_grids": max(1, minimum_verified_unique_grids),
        "status": status,
        "reason": reason,
        "deduplication": "returned_grid_coordinate; one independent sample per returned model grid",
    }


def _window_expected_dates(definition: dict) -> list[str]:
    if definition.get("status") != "OK" or not definition.get("start_date") or not definition.get("end_date"):
        return []
    start = dt.date.fromisoformat(definition["start_date"])
    end = dt.date.fromisoformat(definition["end_date"])
    return [
        (start + dt.timedelta(days=offset)).isoformat()
        for offset in range((end - start).days + 1)
    ]


def lightweight_window_metrics(days: list[dict]) -> dict:
    complete_days = sorted(
        [day for day in days if day.get("complete") and day.get("date")],
        key=lambda day: day["date"],
    )
    values = {
        key: [metric_value(day, source_key) for day in complete_days]
        for key, source_key in (
            ("temperature_mean_c", "temperature_mean_c"),
            ("temperature_max_mean_c", "temperature_max_c"),
            ("night_min_mean_c", "night_min_c"),
            ("precipitation_total_mm", "precipitation_mm"),
            ("snowfall_total_cm", "snowfall_cm"),
            ("wind_gust_max_kmh", "wind_gust_max_kmh"),
        )
    }
    for key in values:
        values[key] = [value for value in values[key] if value is not None]
    night_values = values["night_min_mean_c"]
    solar_values = []
    solar_variables = set()
    solar_units = set()
    for day in complete_days:
        solar = day.get("solar_metric")
        if isinstance(solar, dict) and isinstance(solar.get("value"), (int, float)):
            solar_values.append(float(solar["value"]))
            if solar.get("variable"):
                solar_variables.add(solar["variable"])
            if solar.get("unit"):
                solar_units.add(solar["unit"])
    diurnal_values = []
    for day in complete_days:
        high = metric_value(day, "temperature_max_c")
        low = metric_value(day, "temperature_min_c")
        if high is not None and low is not None:
            diurnal_values.append(high - low)
    first_three = [metric_value(day, "temperature_mean_c") for day in complete_days[:3]]
    last_three = [metric_value(day, "temperature_mean_c") for day in complete_days[-3:]]
    first_three = [value for value in first_three if value is not None]
    last_three = [value for value in last_three if value is not None]
    first_mean = safe_mean(first_three)
    last_mean = safe_mean(last_three)
    trend_delta = round(last_mean - first_mean, 3) if first_mean is not None and last_mean is not None else None
    if trend_delta is None:
        trend_direction = "UNDETERMINED"
    elif trend_delta >= 0.5:
        trend_direction = "WARMING"
    elif trend_delta <= -0.5:
        trend_direction = "COOLING"
    else:
        trend_direction = "NEAR_FLAT"
    solar_variable = next(iter(solar_variables), None) if len(solar_variables) == 1 else "mixed" if solar_variables else None
    solar_unit = next(iter(solar_units), None) if len(solar_units) == 1 else "mixed" if solar_units else None
    return {
        "temperature_mean_c": safe_mean(values["temperature_mean_c"]),
        "temperature_max_mean_c": safe_mean(values["temperature_max_mean_c"]),
        "night_min_mean_c": safe_mean(night_values),
        "absolute_min_night_c": round(min(night_values), 3) if night_values else None,
        "nights_below_15c": sum(value < 15 for value in night_values),
        "nights_below_10c": sum(value < 10 for value in night_values),
        "nights_below_5c": sum(value < 5 for value in night_values),
        "nights_below_2c": sum(value < 2 for value in night_values),
        "nights_below_0c": sum(value < 0 for value in night_values),
        "diurnal_temperature_range_mean_c": safe_mean(diurnal_values),
        "precipitation_total_mm": safe_sum(values["precipitation_total_mm"]),
        "snowfall_total_cm": safe_sum(values["snowfall_total_cm"]),
        "average_daily_sunshine_duration_seconds": (
            safe_mean(solar_values) if solar_variable == "sunshine_duration" else None
        ),
        "average_daily_shortwave_radiation_w_m2": (
            safe_mean(solar_values) if solar_variable == "shortwave_radiation" else None
        ),
        "solar_variable": solar_variable,
        "solar_unit": solar_unit,
        "max_wind_gust_kmh": round(max(values["wind_gust_max_kmh"]), 3) if values["wind_gust_max_kmh"] else None,
        "strong_wind_day_count": sum(value >= 50 for value in values["wind_gust_max_kmh"]),
        "temperature_trend": {
            "first_3_days_mean_temperature_c": first_mean,
            "last_3_days_mean_temperature_c": last_mean,
            "last_3_minus_first_3_mean_temperature_c": trend_delta,
            "direction": trend_direction,
            "method": "last_3_complete_daily_means_minus_first_3_complete_daily_means; threshold=0.5C",
        },
    }


def lightweight_window_summary(days: list[dict], definition: dict, *, allow_partial: bool = False) -> dict:
    expected_dates = _window_expected_dates(definition)
    if not expected_dates:
        return {
            "status": _definition_window_status(definition),
            "start_date": definition.get("start_date"),
            "end_date": definition.get("end_date"),
            "expected_days": 0,
            "days_available": 0,
            "missing_dates": [],
            "metrics": None,
            "reason": definition.get("reason") or "WINDOW_UNAVAILABLE",
        }
    start_date = expected_dates[0]
    end_date = expected_dates[-1]
    selected = [
        day for day in days
        if isinstance(day.get("date"), str) and start_date <= day["date"] <= end_date
    ]
    available_complete_dates = {day["date"] for day in selected if day.get("complete")}
    missing_dates = [date_value for date_value in expected_dates if date_value not in available_complete_dates]
    incomplete_dates = sorted(
        day["date"] for day in selected
        if day.get("date") and not day.get("complete")
    )
    complete = not missing_dates and not incomplete_dates
    if complete:
        status = "OK"
    elif available_complete_dates and allow_partial:
        status = "PARTIAL"
    else:
        status = "INVALID"
    return {
        "status": status,
        "start_date": start_date,
        "end_date": end_date,
        "expected_days": len(expected_dates),
        "days_available": len(available_complete_dates),
        "missing_dates": missing_dates,
        "incomplete_dates": incomplete_dates,
        "metrics": lightweight_window_metrics(selected) if available_complete_dates else None,
        "reason": None if complete else "WINDOW_INCOMPLETE",
    }


def aggregate_grid_window(records: list[dict], definition: dict, *, allow_partial: bool = False) -> dict:
    """Aggregate one window over unique grids with equal grid weighting."""
    if not history_forward_window_applicable(definition):
        return {
            "status": HISTORY_FORWARD_WINDOW_NOT_APPLICABLE,
            "start_date": None,
            "end_date": None,
            "expected_days": 0,
            "days_available": 0,
            "missing_dates": [],
            "grid_count": 0,
            "metrics": None,
            "reason": definition.get("reason") or "WINDOW_AFTER_CUTOFF",
            "aggregation": None,
        }
    records = [entry["record"] for entry in deduplicate_grid_records(records)]
    if not records:
        return lightweight_window_summary([], definition, allow_partial=allow_partial) | {
            "status": "INVALID",
            "reason": "NO_VALID_UNIQUE_GRID_RECORDS",
        }
    summaries = [lightweight_window_summary(record.get("daily", []), definition, allow_partial=allow_partial) for record in records]
    usable = [item for item in summaries if item.get("metrics")]
    expected_days = max((item.get("expected_days", 0) for item in summaries), default=0)
    missing_dates = sorted({value for item in summaries for value in item.get("missing_dates", [])})
    status_values = {item.get("status") for item in summaries}
    if all(value == "OK" for value in status_values) and len(usable) == len(records):
        status = "OK"
    elif usable and allow_partial:
        status = "PARTIAL"
    else:
        status = "INVALID"
    if not usable:
        return {
            "status": status,
            "start_date": definition.get("start_date"),
            "end_date": definition.get("end_date"),
            "expected_days": expected_days,
            "days_available": 0,
            "missing_dates": missing_dates,
            "grid_count": len(records),
            "metrics": None,
            "reason": "NO_COMPLETE_UNIQUE_GRID_WINDOW",
        }
    metrics = {}
    mean_keys = [
        key for key in LIGHTWEIGHT_WINDOW_METRIC_KEYS
        if key not in {"absolute_min_night_c", "max_wind_gust_kmh", "strong_wind_day_count"}
    ]
    for key in mean_keys:
        values = [item["metrics"].get(key) for item in usable if isinstance(item["metrics"].get(key), (int, float))]
        metrics[key] = safe_mean([float(value) for value in values]) if values else None
    absolute_mins = [item["metrics"].get("absolute_min_night_c") for item in usable if isinstance(item["metrics"].get("absolute_min_night_c"), (int, float))]
    gusts = [item["metrics"].get("max_wind_gust_kmh") for item in usable if isinstance(item["metrics"].get("max_wind_gust_kmh"), (int, float))]
    metrics["absolute_min_night_c"] = round(min(absolute_mins), 3) if absolute_mins else None
    metrics["max_wind_gust_kmh"] = round(max(gusts), 3) if gusts else None
    trend_items = [item["metrics"].get("temperature_trend") or {} for item in usable]
    trend_delta_values = [item.get("last_3_minus_first_3_mean_temperature_c") for item in trend_items if isinstance(item.get("last_3_minus_first_3_mean_temperature_c"), (int, float))]
    first_values = [item.get("first_3_days_mean_temperature_c") for item in trend_items if isinstance(item.get("first_3_days_mean_temperature_c"), (int, float))]
    last_values = [item.get("last_3_days_mean_temperature_c") for item in trend_items if isinstance(item.get("last_3_days_mean_temperature_c"), (int, float))]
    trend_delta = safe_mean([float(value) for value in trend_delta_values]) if trend_delta_values else None
    trend_direction = "UNDETERMINED" if trend_delta is None else "WARMING" if trend_delta >= 0.5 else "COOLING" if trend_delta <= -0.5 else "NEAR_FLAT"
    metrics["temperature_trend"] = {
        "first_3_days_mean_temperature_c": safe_mean([float(value) for value in first_values]) if first_values else None,
        "last_3_days_mean_temperature_c": safe_mean([float(value) for value in last_values]) if last_values else None,
        "last_3_minus_first_3_mean_temperature_c": trend_delta,
        "direction": trend_direction,
        "method": "equal_mean_of_unique_grid_window_trends; threshold=0.5C",
    }
    return {
        "status": status,
        "start_date": definition.get("start_date"),
        "end_date": definition.get("end_date"),
        "expected_days": expected_days,
        "days_available": min((item.get("days_available", 0) for item in usable), default=0),
        "missing_dates": missing_dates,
        "grid_count": len(records),
        "metrics": metrics,
        "reason": None if status == "OK" else "UNIQUE_GRID_WINDOW_PARTIAL",
        "aggregation": "equal_mean_over_unique_returned_model_grids; min/max fields preserve spatial extremes",
    }


def build_region_history_subregion(
    config: dict,
    region_id: str,
    subregion_id: str,
    point_results: dict[str, dict],
    forecast_date: dt.date,
) -> dict:
    registry_item = region_subregion_registry(config, region_id).get(subregion_id) or {}
    candidate_ids = region_subregion_point_ids(config, region_id, subregion_id)
    minimum_grids = registry_item.get("minimum_verified_unique_grids", 1)
    consistent_point_ids = [
        point_id
        for point_id in candidate_ids
        if point_id in point_results
        and point_results[point_id].get("status") == "OK"
        and (point_results[point_id].get("same_grid_qa") or {}).get("final_status") == "PASS"
    ]
    years = {}
    sampling_by_year = {}
    grid_sets = {}
    for year in HISTORY_FORWARD_YEARS:
        records = {
            point_id: point_results[point_id]["years"][str(year)]
            for point_id in consistent_point_ids
            if str(year) in point_results[point_id].get("years", {})
        }
        sampling = grid_sampling_summary(
            config,
            candidate_ids,
            records,
            minimum_verified_unique_grids=minimum_grids,
        )
        sampling_by_year[str(year)] = sampling
        unique_records = deduplicate_grid_records(list(records.values()))
        grid_sets[str(year)] = [entry["grid_cell_id"] for entry in unique_records]
        definitions = history_forward_windows_for_year(forecast_date, year)
        year_view = {
            "status": "OK",
            "sampling": sampling,
        }
        for key, definition in definitions.items():
            year_view[key] = aggregate_grid_window(
                [entry["record"] for entry in unique_records],
                definition,
                allow_partial=False,
            )
            if not history_forward_window_applicable(definition):
                continue
            if year_view[key]["status"] != "OK":
                year_view["status"] = "INVALID"
        years[str(year)] = year_view
    same_grid_set = bool(grid_sets) and len({json.dumps(value, sort_keys=True) for value in grid_sets.values()}) == 1
    all_year_windows_ok = all(
        years.get(str(year), {}).get("status") == "OK"
        for year in HISTORY_FORWARD_YEARS
    )
    all_sampling_ok = all(
        item.get("status") == "OK"
        for item in sampling_by_year.values()
    )
    if not consistent_point_ids:
        status = "INVALID"
        reason = "NO_POINT_WITH_THREE_YEAR_SAME_GRID_QA"
    elif not same_grid_set:
        status = "INVALID"
        reason = f"{region_id.upper()}_SUBREGION_GRID_SET_MISMATCH"
    elif all_year_windows_ok and all_sampling_ok:
        status = "OK"
        reason = None
    else:
        status = "PARTIAL"
        reason = "INSUFFICIENT_VERIFIED_UNIQUE_GRIDS_OR_WINDOW_DATA"
    first_sampling = sampling_by_year.get(str(HISTORY_FORWARD_YEARS[0]), {})
    return {
        "subregion": subregion_id,
        "name": registry_item.get("name", subregion_id),
        "status": status,
        "usable_for_main_chain": bool(consistent_point_ids),
        "cross_year_comparison_usable": status == "OK",
        "point_ids": candidate_ids,
        "verified_point_ids": [
            point_id for point_id in candidate_ids
            if config.get("points", {}).get(point_id, {}).get("status") == "VERIFIED"
        ],
        "consistent_point_ids": consistent_point_ids,
        "sampling": {
            "status": status if status in {"INVALID", "PARTIAL"} else first_sampling.get("status", "INVALID"),
            "minimum_verified_unique_grids": minimum_grids,
            "by_year": sampling_by_year,
            "grid_sets_by_year": grid_sets,
            "same_unique_grid_set_across_years": same_grid_set,
        },
        "years": years,
        "reason": reason,
    }


def build_siguniang_history_subregion(
    config: dict,
    subregion_id: str,
    point_results: dict[str, dict],
    forecast_date: dt.date,
) -> dict:
    return build_region_history_subregion(config, "siguniang", subregion_id, point_results, forecast_date)


def build_jiuzhaigou_history_subregion(
    config: dict,
    subregion_id: str,
    point_results: dict[str, dict],
    forecast_date: dt.date,
) -> dict:
    return build_region_history_subregion(config, "jiuzhaigou", subregion_id, point_results, forecast_date)


def equal_mean_subregion_window(
    items: list[dict],
    definition: dict,
    *,
    region_id: str = "siguniang",
) -> dict:
    if not history_forward_window_applicable(definition):
        return {
            "status": HISTORY_FORWARD_WINDOW_NOT_APPLICABLE,
            "start_date": None,
            "end_date": None,
            "expected_days": 0,
            "days_available": 0,
            "missing_dates": [],
            "metrics": None,
            "reason": definition.get("reason") or "WINDOW_AFTER_CUTOFF",
            "available_subregions": 0,
            "expected_subregions": len(items),
        }
    if not items:
        expected_dates = _window_expected_dates(definition)
        return {
            "status": "INVALID",
            "start_date": definition.get("start_date"),
            "end_date": definition.get("end_date"),
            "expected_days": len(expected_dates),
            "days_available": 0,
            "missing_dates": expected_dates,
            "metrics": None,
            "reason": f"NO_USABLE_{region_id.upper()}_SUBREGION",
        }
    metric_items = [
        item.get("metrics") or {
            key: item.get(key)
            for key in LIGHTWEIGHT_WINDOW_METRIC_KEYS
            if key in item
        }
        for item in items
        if item.get("status") in {"OK", "PARTIAL"}
        and (item.get("metrics") or any(key in item for key in LIGHTWEIGHT_WINDOW_METRIC_KEYS))
    ]
    if len(metric_items) != len(items):
        missing_dates = sorted({date_value for item in items for date_value in item.get("missing_dates", [])})
        if not metric_items and not missing_dates:
            missing_dates = _window_expected_dates(definition)
        status = "PARTIAL" if metric_items else "INVALID"
        return {
            "status": status,
            "start_date": definition.get("start_date"),
            "end_date": definition.get("end_date"),
            "expected_days": max((item.get("expected_days", 0) for item in items), default=0),
            "days_available": min((item.get("days_available", 0) for item in items), default=0),
            "missing_dates": missing_dates,
            "metrics": None,
            "reason": (
                f"{region_id.upper()}_COMPOSITE_SUBREGION_WINDOW_PARTIAL"
                if status == "PARTIAL"
                else "NO_COMPLETE_UNIQUE_GRID_WINDOW"
            ),
            "available_subregions": len(metric_items),
            "expected_subregions": len(items),
        }
    metrics = {}
    for key in LIGHTWEIGHT_WINDOW_METRIC_KEYS:
        if key in {"absolute_min_night_c", "max_wind_gust_kmh"}:
            continue
        values = [item.get(key) for item in metric_items if isinstance(item.get(key), (int, float))]
        metrics[key] = safe_mean([float(value) for value in values]) if values else None
    absolute_mins = [item.get("absolute_min_night_c") for item in metric_items if isinstance(item.get("absolute_min_night_c"), (int, float))]
    gusts = [item.get("max_wind_gust_kmh") for item in metric_items if isinstance(item.get("max_wind_gust_kmh"), (int, float))]
    metrics["absolute_min_night_c"] = round(min(absolute_mins), 3) if absolute_mins else None
    metrics["max_wind_gust_kmh"] = round(max(gusts), 3) if gusts else None
    trend_items = [
        item.get("temperature_trend")
        for item in metric_items
        if isinstance(item.get("temperature_trend"), dict)
    ]
    trend_first = [
        item.get("first_3_days_mean_temperature_c")
        for item in trend_items
        if isinstance(item.get("first_3_days_mean_temperature_c"), (int, float))
    ]
    trend_last = [
        item.get("last_3_days_mean_temperature_c")
        for item in trend_items
        if isinstance(item.get("last_3_days_mean_temperature_c"), (int, float))
    ]
    trend_delta_values = [
        item.get("last_3_minus_first_3_mean_temperature_c")
        for item in trend_items
        if isinstance(item.get("last_3_minus_first_3_mean_temperature_c"), (int, float))
    ]
    trend_delta = safe_mean([float(value) for value in trend_delta_values])
    if trend_delta is None:
        trend_direction = "UNDETERMINED"
    elif trend_delta >= 0.5:
        trend_direction = "WARMING"
    elif trend_delta <= -0.5:
        trend_direction = "COOLING"
    else:
        trend_direction = "NEAR_FLAT"
    metrics["temperature_trend"] = {
        "first_3_days_mean_temperature_c": safe_mean([float(value) for value in trend_first]),
        "last_3_days_mean_temperature_c": safe_mean([float(value) for value in trend_last]),
        "last_3_minus_first_3_mean_temperature_c": trend_delta,
        "direction": trend_direction,
        "method": "equal_mean_of_subregion_window_trends; threshold=0.5C",
    }
    aggregate_status = "OK" if all(item.get("status") == "OK" for item in items) else "PARTIAL"
    return {
        "status": aggregate_status,
        "start_date": definition.get("start_date"),
        "end_date": definition.get("end_date"),
        "expected_days": max(item.get("expected_days", 0) for item in items),
        "days_available": min(item.get("days_available", 0) for item in items),
        "missing_dates": sorted({date_value for item in items for date_value in item.get("missing_dates", [])}),
        "metrics": metrics,
        "reason": None if aggregate_status == "OK" else f"{region_id.upper()}_COMPOSITE_SUBREGION_WINDOW_PARTIAL",
        "aggregation": f"equal_mean_of_{region_id}_subregions; no point-count weighting",
    }


def build_region_history_composite(
    subregions: dict[str, dict],
    subregion_keys: tuple[str, ...],
    forecast_date: dt.date,
    *,
    region_id: str,
) -> dict:
    definitions_by_year = {
        str(year): history_forward_windows_for_year(forecast_date, year)
        for year in HISTORY_FORWARD_YEARS
    }
    years = {}
    for year in HISTORY_FORWARD_YEARS:
        year_view = {"status": "OK"}
        for key, definition in definitions_by_year[str(year)].items():
            items = [
                subregions[subregion_id].get("years", {}).get(str(year), {}).get(key, {})
                for subregion_id in subregion_keys
                if subregion_id in subregions
            ]
            year_view[key] = equal_mean_subregion_window(items, definition, region_id=region_id)
            if not history_forward_window_applicable(definition):
                continue
            if year_view[key]["status"] != "OK":
                year_view["status"] = "PARTIAL"
        years[str(year)] = year_view
    statuses = {subregions.get(key, {}).get("status", "INVALID") for key in subregion_keys}
    if statuses == {"OK"} and all(item.get("status") == "OK" for item in years.values()):
        status = "OK"
        reason = None
    elif statuses & {"OK", "PARTIAL"}:
        status = "PARTIAL"
        reason = f"{region_id.upper()}_COMPOSITE_REQUIRES_ALL_SUBREGIONS"
    else:
        status = "INVALID"
        reason = f"NO_USABLE_{region_id.upper()}_SUBREGION"
    subregion_names = ", ".join(subregion_keys)
    return {
        "status": status,
        "usable_for_main_chain": status == "OK",
        "cross_year_comparison_usable": status == "OK",
        "aggregation": f"equal_mean_of_subregions; {subregion_names} each weight=1/{len(subregion_keys)}",
        "subregion_statuses": {key: subregions.get(key, {}).get("status", "INVALID") for key in subregion_keys},
        "missing_or_partial_subregions": [
            key for key in subregion_keys
            if subregions.get(key, {}).get("status") != "OK"
        ],
        "years": years,
        "reason": reason,
    }


def build_siguniang_history_composite(subregions: dict[str, dict], forecast_date: dt.date) -> dict:
    return build_region_history_composite(
        subregions,
        SIGUNIANG_SUBREGION_KEYS,
        forecast_date,
        region_id="siguniang",
    )


def build_jiuzhaigou_history_composite(subregions: dict[str, dict], forecast_date: dt.date) -> dict:
    return build_region_history_composite(
        subregions,
        JIUZHAIGOU_SUBREGION_KEYS,
        forecast_date,
        region_id="jiuzhaigou",
    )


def compact_history_forward_year(record: dict) -> dict:
    item = compact_record(record)
    for key in HISTORY_FORWARD_WINDOW_KEYS:
        if key in record:
            item[key] = copy.deepcopy(record[key])
    return item


def run_history_forward(
    config: dict,
    client: ApiClient,
    generated_at: str,
    data_date: str,
    forecast_date: dt.date,
    *,
    refresh_history: bool = False,
    cache_dir: Path | None = None,
) -> dict:
    """Fetch real weather after today's calendar date for the reference years."""
    points = active_points(config)
    window_definitions = history_forward_window_definitions(forecast_date)
    applicable_window_keys = history_forward_applicable_window_keys(window_definitions)
    windows_closed = not applicable_window_keys
    if windows_closed:
        log("HISTORY_FORWARD WINDOWS CLOSED: every rolling window is past the cutoff date")
    regions = {}
    point_results = {}
    all_records = []
    expected_point_ids = history_forward_point_ids(config)
    cache_stats = history_cache_stats()

    for point_id in expected_point_ids:
        point = points[point_id]
        years = {}
        for year in HISTORY_FORWARD_YEARS:
            year_windows = history_forward_windows_for_year(forecast_date, year)
            valid_window_ranges = [
                definition for definition in year_windows.values()
                if definition.get("status") == "OK"
            ]
            if not valid_window_ranges:
                record = invalid_record(
                    point=point,
                    source="Open-Meteo",
                    endpoint=OPEN_METEO_ENDPOINTS["history"],
                    model=HISTORY_MODEL,
                    request_params={"models": HISTORY_MODEL_PARAMETER, "year": year},
                    reason="HISTORY_FORWARD_AFTER_CUTOFF",
                )
            else:
                start_date = min(item["start_date"] for item in valid_window_ranges)
                end_date = max(item["end_date"] for item in valid_window_ranges)
                record = history_cache_record_or_fetch(
                    config,
                    client,
                    point,
                    year,
                    start_date,
                    end_date,
                    refresh_history=refresh_history,
                    cache_dir=cache_dir,
                    log_label=f"{point_id}:HISTORY_FORWARD {year}",
                )
            # A closed season yields an invalid record with no daily series, while the
            # year schema still requires the key; publish an explicit empty list.
            record.setdefault("daily", [])
            record["window_definitions"] = year_windows
            for key, definition in year_windows.items():
                record[key] = history_forward_window_summary(record.get("daily", []), definition)
                if record[key]["status"] == "OK":
                    log(f"[{point_id}] HISTORY_FORWARD {year} {key} OK")
                else:
                    log(f"[{point_id}] HISTORY_FORWARD {year} {key} {record[key]['status']}")
            if record.get("status") != "PASS":
                for key, definition in year_windows.items():
                    if not history_forward_window_applicable(definition):
                        continue
                    if record[key]["status"] == "OK":
                        record[key]["status"] = "INVALID"
                        record[key]["usable_for_cross_year_comparison"] = False
                        record[key]["reason"] = "HISTORY_FORWARD_YEAR_INVALID"
            years[str(year)] = record
            all_records.append(record)
            update_history_cache_stats(cache_stats, record)
        same_grid_qa = history_forward_same_grid_qa(years)
        windows_ok = not windows_closed and all(
            all(years[str(year)].get(key, {}).get("status") == "OK" for key in applicable_window_keys)
            for year in HISTORY_FORWARD_YEARS
        )
        if windows_closed:
            point_status = HISTORY_FORWARD_WINDOW_NOT_APPLICABLE
        elif same_grid_qa["final_status"] == "PASS" and windows_ok:
            point_status = "OK"
        else:
            point_status = "FAILED"
        point_results[point_id] = {
            "point_id": point_id,
            "point": {
                "name": point["name"],
                "region": point["region"],
                "status": point["status"],
                "subregion": point.get("subregion"),
            },
            "status": point_status,
            "usable_for_main_chain": not windows_closed,
            "cross_year_comparison_usable": point_status == "OK",
            "same_grid_qa": same_grid_qa,
            "reason": HISTORY_FORWARD_WINDOW_CLOSED_REASON if windows_closed else None,
            "years": years,
        }

    for region_id, region_config in config.get("regions", {}).items():
        core_id = region_config.get("core_point_id")
        core_result = point_results.get(core_id) if core_id else None
        if not core_result:
            regions[region_id] = {
                "region": region_id,
                "core_point_id": core_id,
                "status": "UNAVAILABLE",
                "usable_for_main_chain": False,
                "cross_year_comparison_usable": False,
                "same_grid_qa": None,
                "years": {},
                "reason": "NO_VERIFIED_CORE_POINT",
            }
            log(f"[{region_id}] HISTORY_FORWARD SKIPPED: PROVISIONAL")
            continue
        regions[region_id] = {
            "region": region_id,
            "core_point_id": core_id,
            "status": core_result["status"],
            "usable_for_main_chain": core_result["usable_for_main_chain"],
            "cross_year_comparison_usable": core_result["cross_year_comparison_usable"],
            "same_grid_qa": core_result["same_grid_qa"],
            # Keep the v1.1 core-point paths stable for existing readers.
            "years": {
                year: compact_history_forward_year(record)
                for year, record in core_result["years"].items()
            },
            "reason": (
                None
                if core_result["status"] == "OK"
                else core_result.get("reason") or "HISTORY_FORWARD_NOT_USABLE"
            ),
        }

    registered_subregions = {}
    registered_composites = {}
    for registered_region_id, subregion_keys in SUBREGION_KEYS_BY_REGION.items():
        registry = region_subregion_registry(config, registered_region_id)
        if not registry:
            continue
        subregions = {
            subregion_id: build_region_history_subregion(
                config,
                registered_region_id,
                subregion_id,
                point_results,
                forecast_date,
            )
            for subregion_id in subregion_keys
            if subregion_id in registry
        }
        composite = build_region_history_composite(
            subregions,
            subregion_keys,
            forecast_date,
            region_id=registered_region_id,
        )
        registered_subregions[registered_region_id] = subregions
        registered_composites[registered_region_id] = composite
        if registered_region_id in regions:
            regions[registered_region_id]["subregions"] = subregions
            regions[registered_region_id]["composite"] = composite
            regions[registered_region_id]["subregion_aggregation_status"] = composite["status"]
            if regions[registered_region_id]["status"] != HISTORY_FORWARD_WINDOW_NOT_APPLICABLE:
                regions[registered_region_id]["status"] = (
                    "FAILED"
                    if regions[registered_region_id]["status"] == "FAILED"
                    else "OK" if composite["status"] == "OK" else "PARTIAL"
                )
            regions[registered_region_id]["cross_year_comparison_usable"] = composite["cross_year_comparison_usable"]
            regions[registered_region_id]["reason"] = (
                None
                if regions[registered_region_id]["status"] == "OK"
                else composite.get("reason")
            )
        for subregion_id, item in subregions.items():
            log(f"[{registered_region_id}/{subregion_id}] HISTORY_FORWARD SUBREGION {item['status']}")
        log(f"[{registered_region_id}/composite] HISTORY_FORWARD {composite['status']}")

    enabled_regions = [item for item in regions.values() if item.get("usable_for_main_chain")]
    if windows_closed:
        module_status_value = "SKIPPED"
    elif not enabled_regions or any(item.get("status") == "FAILED" for item in enabled_regions):
        module_status_value = "FAILED"
    elif any(item.get("status") == "PARTIAL" for item in enabled_regions):
        module_status_value = "PARTIAL"
    else:
        module_status_value = "OK"
    partial_points = sum(
        item.get("status") != "OK"
        for subregions in registered_subregions.values()
        for item in subregions.values()
    ) + sum(
        composite.get("status") == "PARTIAL"
        for composite in registered_composites.values()
    )
    aggregation_metadata = {
        region_id: {
            "subregions": list(subregions),
            "composite_status": registered_composites[region_id].get("status"),
            "deduplication": "returned_grid_coordinate; one independent sample per grid",
        }
        for region_id, subregions in registered_subregions.items()
    }
    return module_header(
        "history_forward",
        generated_at,
        data_date,
        module_status_value,
        endpoint=OPEN_METEO_ENDPOINTS["history"],
        model=HISTORY_MODEL,
        model_parameter=HISTORY_MODEL_PARAMETER,
        history_years=list(HISTORY_FORWARD_YEARS),
        forecast_date=forecast_date.isoformat(),
        anchor_date=forecast_date.isoformat(),
        cutoff_date=window_definitions[-1]["cutoff_date"],
        window_definitions=window_definitions,
        interpretation_boundary="Historical weather after the current calendar date; weather reference only.",
        points=point_results,
        regions=regions,
        excluded_points=excluded_points(config),
        successful_fetches=0 if windows_closed else sum(record.get("status") == "PASS" for record in all_records),
        failed_fetches=0 if windows_closed else sum(record.get("status") != "PASS" for record in all_records),
        expected_fetches=0 if windows_closed else len(expected_point_ids) * len(HISTORY_FORWARD_YEARS),
        partial_points=partial_points,
        siguniang_aggregation=aggregation_metadata.get("siguniang", {}),
        jiuzhaigou_aggregation=aggregation_metadata.get("jiuzhaigou", {}),
        subregion_aggregations=aggregation_metadata,
        history_cache={
            "enabled": True,
            "directory": history_cache_relative_path(Path(cache_dir) if cache_dir is not None else HISTORY_CACHE_DIR),
            "refresh_requested": refresh_history,
            **cache_stats,
        },
    )


def failed_history_forward_module(
    config: dict,
    generated_at: str,
    data_date: str,
    forecast_date: dt.date,
    error: Exception,
) -> dict:
    reason = f"{type(error).__name__}:{error}"
    return module_header(
        "history_forward",
        generated_at,
        data_date,
        "FAILED",
        endpoint=OPEN_METEO_ENDPOINTS["history"],
        model=HISTORY_MODEL,
        model_parameter=HISTORY_MODEL_PARAMETER,
        history_years=list(HISTORY_FORWARD_YEARS),
        forecast_date=forecast_date.isoformat(),
        anchor_date=forecast_date.isoformat(),
        cutoff_date=history_forward_window_definitions(forecast_date)[-1]["cutoff_date"],
        window_definitions=history_forward_window_definitions(forecast_date),
        interpretation_boundary="Historical weather after the current calendar date; weather reference only.",
        points={},
        regions={},
        excluded_points=excluded_points(config),
        successful_fetches=0,
        failed_fetches=0,
        expected_fetches=len(history_forward_point_ids(config)) * len(HISTORY_FORWARD_YEARS),
        error=reason,
    )


def metric_value(item: dict, key: str) -> float | None:
    value = item.get(key)
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def consecutive_cold_night_sequences(days: list[dict], threshold: float) -> list[dict]:
    """Return calendar-contiguous runs whose daily night minimum is below a threshold."""
    complete_days = sorted(
        (day for day in days if day.get("complete") and day.get("date")),
        key=lambda day: day["date"],
    )
    sequences = []
    current: list[dict] = []
    previous_date: dt.date | None = None

    def flush() -> None:
        if current:
            sequences.append({
                "start_date": current[0]["date"],
                "end_date": current[-1]["date"],
                "nights": len(current),
            })

    for day in complete_days:
        day_date = dt.date.fromisoformat(day["date"])
        night_min = metric_value(day, "night_min_c")
        is_contiguous = previous_date is not None and day_date == previous_date + dt.timedelta(days=1)
        if night_min is not None and night_min < threshold and (not current or is_contiguous):
            current.append(day)
        else:
            flush()
            current = []
            if night_min is not None and night_min < threshold:
                current.append(day)
        previous_date = day_date
    flush()
    return sequences


def period_metrics(days: list[dict]) -> dict:
    complete_days = [day for day in days if day.get("complete")]
    numeric_keys = {
        "temperature_min_c": "temperature_min_c",
        "temperature_max_c": "temperature_max_c",
        "temperature_mean_c": "temperature_mean_c",
        "night_min_c": "night_min_c",
        "precipitation_mm": "precipitation_mm",
        "snowfall_cm": "snowfall_cm",
        "cloud_cover_mean_pct": "cloud_cover_mean_pct",
        "cloud_cover_low_mean_pct": "cloud_cover_low_mean_pct",
        "wind_speed_mean_kmh": "wind_speed_mean_kmh",
        "wind_gust_max_kmh": "wind_gust_max_kmh",
    }
    metrics = {key: None for key in numeric_keys}
    for key in ("temperature_min_c", "temperature_max_c", "temperature_mean_c", "night_min_c", "cloud_cover_mean_pct", "cloud_cover_low_mean_pct", "wind_speed_mean_kmh"):
        values = [metric_value(day, key) for day in complete_days]
        values = [value for value in values if value is not None]
        metrics[key] = round(mean(values), 3) if values else None
    for key in ("precipitation_mm", "snowfall_cm"):
        values = [metric_value(day, key) for day in complete_days]
        values = [value for value in values if value is not None]
        metrics[key] = round(sum(values), 3) if values else None
    gusts = [metric_value(day, "wind_gust_max_kmh") for day in complete_days]
    gusts = [value for value in gusts if value is not None]
    metrics["wind_gust_max_kmh"] = round(max(gusts), 3) if gusts else None
    metrics["days_available"] = len(complete_days)
    metrics["threshold_nights"] = {
        f"below_{str(int(threshold))}_c": sum(
            1 for day in complete_days if metric_value(day, "night_min_c") is not None and metric_value(day, "night_min_c") < threshold
        )
        for threshold in THRESHOLDS_C
    }
    metrics["consecutive_cold_nights"] = {
        f"below_{str(int(threshold))}_c": {
            "max_consecutive": max(
                (item["nights"] for item in consecutive_cold_night_sequences(complete_days, threshold)),
                default=0,
            ),
            "sequences": consecutive_cold_night_sequences(complete_days, threshold),
        }
        for threshold in THRESHOLDS_C
    }
    coldness = 0.0
    for day in complete_days:
        daily_mean = metric_value(day, "temperature_mean_c")
        night_min = metric_value(day, "night_min_c")
        if daily_mean is not None:
            coldness += max(0.0, 10.0 - daily_mean)
        if night_min is not None:
            coldness += 2.0 * max(0.0, 5.0 - night_min)
            coldness += 3.0 * max(0.0, 2.0 - night_min)
            coldness += 4.0 * max(0.0, 0.0 - night_min)
    metrics["coldness_index"] = round(coldness, 3)
    metrics["diurnal_temperature_range_c"] = (
        round(metrics["temperature_max_c"] - metrics["temperature_min_c"], 3)
        if metrics["temperature_max_c"] is not None and metrics["temperature_min_c"] is not None
        else None
    )
    solar_values = []
    solar_unit = None
    for day in complete_days:
        solar = day.get("solar_metric")
        if isinstance(solar, dict) and isinstance(solar.get("value"), (int, float)):
            solar_values.append(float(solar["value"]))
            solar_unit = solar.get("unit")
    metrics["solar_metric_total_or_mean"] = round(sum(solar_values), 3) if solar_values else None
    metrics["solar_metric_unit"] = solar_unit
    metrics["period_start"] = complete_days[0]["date"] if complete_days else None
    metrics["period_end"] = complete_days[-1]["date"] if complete_days else None
    return metrics


def weather_event_window_metrics(days: list[dict]) -> dict:
    """Aggregate derived event rows without turning missing values into zeros."""
    complete_days = sorted(
        [day for day in days if day.get("complete") and day.get("date")],
        key=lambda day: day["date"],
    )

    def values(key: str) -> list[float]:
        return [
            value for value in (metric_value(day, key) for day in complete_days)
            if value is not None
        ]

    def count(flag: str) -> int:
        return sum(day.get(flag) is True for day in complete_days)

    gusts = values("wind_gust_max_kmh")
    precipitation = values("precipitation_mm")
    snowfall = values("snowfall_cm")
    dtr = values("dtr_c")
    wind_speed = values("wind_speed_mean_kmh")
    maximum_gust = round(max(gusts), 3) if gusts else None
    maximum_gust_day = next(
        (day for day in complete_days if metric_value(day, "wind_gust_max_kmh") == maximum_gust),
        None,
    )
    level_counts = {
        level: sum(
            (day.get("mechanical_leaf_stress") or {}).get("level") == level
            for day in complete_days
        )
        for level in ("LOW", "MEDIUM", "HIGH")
    }
    stress_levels = [
        (day.get("mechanical_leaf_stress") or {}).get("level")
        for day in complete_days
    ]
    if "HIGH" in stress_levels:
        stress_level = "HIGH"
    elif "MEDIUM" in stress_levels:
        stress_level = "MEDIUM"
    elif "LOW" in stress_levels:
        stress_level = "LOW"
    else:
        stress_level = "UNDETERMINED"
    reasons = sorted({
        reason
        for day in complete_days
        for reason in ((day.get("mechanical_leaf_stress") or {}).get("reasons") or [])
    })
    return {
        "days_available": len(complete_days),
        "period_start": complete_days[0]["date"] if complete_days else None,
        "period_end": complete_days[-1]["date"] if complete_days else None,
        "temperature_mean_c": safe_mean(values("temperature_mean_c")),
        "temperature_min_mean_c": safe_mean(values("temperature_min_c")),
        "temperature_max_mean_c": safe_mean(values("temperature_max_c")),
        "night_min_mean_c": safe_mean(values("night_min_c")),
        "absolute_min_night_c": round(min(values("night_min_c")), 3) if values("night_min_c") else None,
        "precipitation_total_mm": safe_sum(precipitation),
        "snowfall_total_cm": safe_sum(snowfall),
        "precipitation_days": count("rain_day"),
        "snowfall_days": count("snow_day"),
        "max_daily_precipitation_mm": round(max(precipitation), 3) if precipitation else None,
        "max_daily_snowfall_cm": round(max(snowfall), 3) if snowfall else None,
        "wind_speed_mean_kmh": safe_mean(wind_speed),
        "wind_gust_max_kmh": maximum_gust,
        "max_gust_source_point_id": (maximum_gust_day or {}).get("max_gust_source_point_id"),
        "max_gust_source_grid_cell_key": (maximum_gust_day or {}).get("max_gust_source_grid_cell_key"),
        "gust_ge_50_days": count("gust_ge_50"),
        "gust_ge_65_days": count("gust_ge_65"),
        "rain_and_gust_ge_50_days": count("rain_and_gust_ge_50"),
        "snow_and_gust_ge_50_days": count("snow_and_gust_ge_50"),
        "freeze_and_snow_days": count("freeze_and_snow"),
        "freeze_days": count("freeze"),
        "hard_freeze_le_minus5_days": count("hard_freeze_le_minus5"),
        "dtr_mean_c": safe_mean(dtr),
        "dtr_max_c": round(max(dtr), 3) if dtr else None,
        "combined_weather_stress_events": sum(level in {"MEDIUM", "HIGH"} for level in stress_levels),
        "mechanical_leaf_stress": {
            "level": stress_level,
            "reasons": reasons,
            "day_count_by_level": level_counts,
            "rule_version": MECHANICAL_LEAF_STRESS_RULE_VERSION,
        },
    }


def weather_event_window_summary(
    days: list[dict],
    definition: dict,
    *,
    allow_partial: bool = False,
) -> dict:
    if definition.get("status") != "OK" or not definition.get("start_date") or not definition.get("end_date"):
        return {
            "status": "UNAVAILABLE",
            "start_date": definition.get("start_date"),
            "end_date": definition.get("end_date"),
            "expected_days": 0,
            "days_available": 0,
            "missing_dates": [],
            "incomplete_dates": [],
            "metrics": None,
            "reason": definition.get("reason") or "WINDOW_UNAVAILABLE",
        }
    expected_dates = _history_date_list(definition["start_date"], definition["end_date"])
    selected = [
        day for day in days
        if isinstance(day.get("date"), str)
        and definition["start_date"] <= day["date"] <= definition["end_date"]
    ]
    complete_dates = {day["date"] for day in selected if day.get("complete")}
    missing_dates = [value for value in expected_dates if value not in complete_dates]
    incomplete_dates = sorted(
        day["date"] for day in selected
        if day.get("date") and not day.get("complete")
    )
    if not missing_dates and not incomplete_dates:
        status = "OK"
    elif complete_dates and allow_partial:
        status = "PARTIAL"
    else:
        status = "INVALID"
    return {
        "status": status,
        "start_date": definition["start_date"],
        "end_date": definition["end_date"],
        "expected_days": len(expected_dates),
        "days_available": len(complete_dates),
        "missing_dates": missing_dates,
        "incomplete_dates": incomplete_dates,
        "metrics": weather_event_window_metrics(selected) if complete_dates else None,
        "reason": None if status == "OK" else "WEATHER_EVENT_WINDOW_INCOMPLETE",
    }


def mechanical_leaf_stress_window(days: list[dict]) -> dict:
    metrics = weather_event_window_metrics(days)
    return copy.deepcopy(metrics["mechanical_leaf_stress"])


def cooling_episode_candidates(days: list[dict]) -> list[dict]:
    """Find repeatable cooling candidates using a fixed three-day baseline."""
    complete_days = sorted(
        [day for day in days if day.get("complete") and day.get("date")],
        key=lambda day: day["date"],
    )
    candidates = []
    index = COOLING_BASELINE_DAYS
    while index < len(complete_days):
        baseline_days = complete_days[index - COOLING_BASELINE_DAYS:index]
        baseline_dates = [dt.date.fromisoformat(day["date"]) for day in baseline_days]
        if any(
            baseline_dates[offset] != baseline_dates[offset - 1] + dt.timedelta(days=1)
            for offset in range(1, len(baseline_dates))
        ):
            index += 1
            continue
        baseline_means = [metric_value(day, "temperature_mean_c") for day in baseline_days]
        baseline_nights = [metric_value(day, "night_min_c") for day in baseline_days]
        baseline_means = [value for value in baseline_means if value is not None]
        baseline_nights = [value for value in baseline_nights if value is not None]
        current_mean = metric_value(complete_days[index], "temperature_mean_c")
        current_night = metric_value(complete_days[index], "night_min_c")
        baseline_mean = safe_mean(baseline_means)
        baseline_night = safe_mean(baseline_nights)
        mean_drop = baseline_mean - current_mean if baseline_mean is not None and current_mean is not None else None
        night_drop = baseline_night - current_night if baseline_night is not None and current_night is not None else None
        triggered = (
            mean_drop is not None and mean_drop >= COOLING_MIN_MEAN_DROP_C
        ) or (
            night_drop is not None and night_drop >= COOLING_MIN_NIGHT_DROP_C
        )
        if not triggered:
            index += 1
            continue
        start_index = index
        end_index = index
        while end_index + 1 < len(complete_days):
            previous_date = dt.date.fromisoformat(complete_days[end_index]["date"])
            next_day = complete_days[end_index + 1]
            next_date = dt.date.fromisoformat(next_day["date"])
            if next_date != previous_date + dt.timedelta(days=1):
                break
            next_mean = metric_value(next_day, "temperature_mean_c")
            next_night = metric_value(next_day, "night_min_c")
            remains_cold = (
                next_mean is not None
                and baseline_mean is not None
                and next_mean <= baseline_mean - COOLING_ACTIVE_DAY_TOLERANCE_C
            ) or (
                next_night is not None
                and baseline_night is not None
                and next_night <= baseline_night - COOLING_ACTIVE_DAY_TOLERANCE_C
            )
            if not remains_cold:
                break
            end_index += 1
        recovery_index = None
        for candidate_index in range(end_index + 1, min(len(complete_days), end_index + 1 + COOLING_MAX_RECOVERY_DAYS)):
            candidate_day = complete_days[candidate_index]
            previous_date = dt.date.fromisoformat(complete_days[candidate_index - 1]["date"])
            candidate_date = dt.date.fromisoformat(candidate_day["date"])
            if candidate_date != previous_date + dt.timedelta(days=1):
                break
            candidate_mean = metric_value(candidate_day, "temperature_mean_c")
            candidate_night = metric_value(candidate_day, "night_min_c")
            recovered = (
                candidate_mean is not None
                and baseline_mean is not None
                and candidate_mean >= baseline_mean - COOLING_RECOVERY_TOLERANCE_C
            ) or (
                candidate_night is not None
                and baseline_night is not None
                and candidate_night >= baseline_night - COOLING_RECOVERY_TOLERANCE_C
            )
            if recovered:
                recovery_index = candidate_index
                break
        event_days = complete_days[start_index:end_index + 1]
        start_date = event_days[0]["date"]
        end_date = event_days[-1]["date"]
        if candidates:
            previous_end = dt.date.fromisoformat(candidates[-1]["end_date"])
            if (dt.date.fromisoformat(start_date) - previous_end).days <= COOLING_MIN_EPISODE_SEPARATION_DAYS:
                index = end_index + 1
                continue
        metrics = weather_event_window_metrics(event_days)
        episode = {
            "start_date": start_date,
            "end_date": end_date,
            "baseline_temperature_mean_c": baseline_mean,
            "minimum_daily_mean_c": min((metric_value(day, "temperature_mean_c") for day in event_days if metric_value(day, "temperature_mean_c") is not None), default=None),
            "minimum_night_min_c": min((metric_value(day, "night_min_c") for day in event_days if metric_value(day, "night_min_c") is not None), default=None),
            "temperature_drop_c": round(
                baseline_mean - min((metric_value(day, "temperature_mean_c") for day in event_days if metric_value(day, "temperature_mean_c") is not None), default=baseline_mean),
                3,
            ) if baseline_mean is not None else None,
            "dtr_mean_c": metrics.get("dtr_mean_c"),
            "dtr_max_c": metrics.get("dtr_max_c"),
            "precipitation_total_mm": metrics.get("precipitation_total_mm"),
            "snowfall_total_cm": metrics.get("snowfall_total_cm"),
            "wind_gust_max_kmh": metrics.get("wind_gust_max_kmh"),
            "gust_ge_50_days": metrics.get("gust_ge_50_days", 0),
            "gust_ge_65_days": metrics.get("gust_ge_65_days", 0),
            "rain_and_gust_ge_50_days": metrics.get("rain_and_gust_ge_50_days", 0),
            "snow_and_gust_ge_50_days": metrics.get("snow_and_gust_ge_50_days", 0),
            "freeze_days": metrics.get("freeze_days", 0),
            "hard_freeze_days": metrics.get("hard_freeze_le_minus5_days", 0),
            "recovery_status": "OBSERVED" if recovery_index is not None else "NOT_YET_OBSERVED",
            "rule_version": COOLING_EPISODE_RULE_VERSION,
        }
        candidates.append(episode)
        index = end_index + 1
    return candidates


def _weather_event_identity_record(item: dict, point_id: str) -> dict:
    """Rebuild a compact grid record for the existing deduplication helpers."""
    identity = item.get("identity") or {}
    qa = item.get("qa") or {}
    return {
        "point_id": point_id,
        "status": "PASS" if item.get("status") == "OK" else "INVALID",
        "source": item.get("source", identity.get("source", "Open-Meteo")),
        "endpoint": item.get("endpoint", identity.get("endpoint")),
        "model": item.get("model", identity.get("model")),
        "request": {
            "coordinate": copy.deepcopy(identity.get("requested_coordinate")),
            "parameters": copy.deepcopy(item.get("request_parameters") or {}),
        },
        "response": {
            "grid_coordinate": copy.deepcopy(identity.get("returned_grid_coordinate")),
            "returned_elevation": identity.get("returned_elevation"),
            "timezone": identity.get("timezone"),
            "utc_offset_seconds": identity.get("utc_offset_seconds"),
        },
        "qa": {
            "final_status": qa.get("final_status", "PASS" if item.get("status") == "OK" else "INVALID"),
            "grid_distance_km": qa.get("grid_distance_km", identity.get("grid_distance_km")),
            "grid_distance_limit_km": qa.get("grid_distance_limit_km", identity.get("grid_distance_limit_km")),
        },
        "daily": copy.deepcopy(item.get("daily") or []),
    }


def _weather_event_item_from_history_cache(
    config: dict,
    point: dict,
    year: int,
    history_cache: dict,
    cache_update: dict,
) -> dict:
    identity = copy.deepcopy(history_cache.get("identity") or {})
    metadata = history_cache.get("record_metadata") or {}
    qa = copy.deepcopy(metadata.get("qa") or {})
    daily = copy.deepcopy(history_cache.get("daily") or [])
    return {
        "status": "OK" if qa.get("final_status") == "PASS" else "INVALID",
        "source_state": "finalized_history",
        "year": int(year),
        "point_id": point.get("id"),
        "source": identity.get("source", "Open-Meteo"),
        "endpoint": identity.get("endpoint", OPEN_METEO_ENDPOINTS["history"]),
        "model": identity.get("model", HISTORY_MODEL),
        "request_parameters": copy.deepcopy((metadata.get("request") or {}).get("parameters") or {}),
        "identity": identity,
        "qa": qa,
        "daily": daily,
        "metrics": weather_event_window_metrics(daily),
        "cache_update": copy.deepcopy(cache_update),
    }


def _weather_event_item_from_forecast_record(point: dict, record: dict, cutoff_date: dt.date | None) -> dict:
    response = record.get("response") or {}
    qa = copy.deepcopy(record.get("qa") or {})
    daily = []
    for day in record.get("daily") or []:
        value = day.get("date")
        try:
            keep = cutoff_date is None or dt.date.fromisoformat(value) <= cutoff_date
        except (TypeError, ValueError):
            keep = False
        if keep:
            daily.append(derive_weather_event_day(day, "forecast"))
    identity = {
        "namespace": "siguniang_jiuzhaigou",
        "point_id": point.get("id"),
        "source": record.get("source", "Open-Meteo"),
        "endpoint": record.get("endpoint"),
        "model": record.get("model"),
        "requested_coordinate": copy.deepcopy((record.get("request") or {}).get("coordinate")),
        "returned_grid_coordinate": copy.deepcopy(response.get("grid_coordinate")),
        "returned_elevation": response.get("returned_elevation"),
        "grid_distance_km": qa.get("grid_distance_km"),
        "grid_distance_limit_km": qa.get("grid_distance_limit_km"),
        "grid_cell_key": record_grid_cell_key(record),
        "cell_selection": ((record.get("request") or {}).get("parameters") or {}).get("cell_selection"),
        "elevation": ((record.get("request") or {}).get("parameters") or {}).get("elevation"),
        "timezone": response.get("timezone"),
        "utc_offset_seconds": response.get("utc_offset_seconds"),
    }
    return {
        "status": "OK" if record.get("status") == "PASS" and qa.get("final_status", "PASS") == "PASS" else "INVALID",
        "source_state": "forecast",
        "point_id": point.get("id"),
        "source": record.get("source", "Open-Meteo"),
        "endpoint": record.get("endpoint"),
        "model": record.get("model"),
        "request_parameters": copy.deepcopy((record.get("request") or {}).get("parameters") or {}),
        "response": copy.deepcopy(response),
        "identity": identity,
        "qa": qa,
        "daily": daily,
        "metrics": weather_event_window_metrics(daily),
    }


def _aggregate_weather_event_grid_days(unique_entries: list[dict], source_state: str) -> list[dict]:
    by_date: dict[str, list[tuple[dict, dict]]] = {}
    for entry in unique_entries:
        record = entry.get("record") or {}
        for day in record.get("daily") or []:
            if day.get("date"):
                by_date.setdefault(day["date"], []).append((day, entry))
    continuous_keys = (
        "temperature_mean_c",
        "temperature_min_c",
        "temperature_max_c",
        "night_min_c",
        "dtr_c",
        "precipitation_mm",
        "snowfall_cm",
        "wind_speed_mean_kmh",
    )
    output = []
    total_grids = len(unique_entries)
    for day_date in sorted(by_date):
        rows = by_date[day_date]
        item = {
            "date": day_date,
            "source_state": source_state,
            "complete": total_grids > 0 and len(rows) == total_grids and all(day.get("complete") for day, _ in rows),
            "available_grid_count": len(rows),
            "total_unique_grid_count": total_grids,
        }
        for key in continuous_keys:
            values = [metric_value(day, key) for day, _ in rows]
            values = [value for value in values if value is not None]
            item[key] = safe_mean(values)
        gust_rows = [
            (metric_value(day, "wind_gust_max_kmh"), entry)
            for day, entry in rows
            if metric_value(day, "wind_gust_max_kmh") is not None
        ]
        if gust_rows:
            gust, gust_entry = max(gust_rows, key=lambda value: value[0])
            item["wind_gust_max_kmh"] = round(gust, 3)
            item["max_gust_source_point_id"] = gust_entry.get("representative_point_id")
            item["max_gust_source_grid_cell_key"] = gust_entry.get("grid_cell_id")
        else:
            item["wind_gust_max_kmh"] = None
            item["max_gust_source_point_id"] = None
            item["max_gust_source_grid_cell_key"] = None
        event_counts = {}
        for flag in WEATHER_EVENT_FLAG_KEYS:
            flag_values = [day.get(flag) for day, _ in rows if day.get(flag) is not None]
            triggered = sum(value is True for value in flag_values)
            event_counts[flag] = {
                "any_grid_event": True if triggered else False if flag_values else None,
                "triggered_unique_grid_count": triggered,
                "total_unique_grid_count": total_grids,
            }
            item[flag] = event_counts[flag]["any_grid_event"]
        item["event_counts"] = event_counts
        item["mechanical_leaf_stress"] = mechanical_leaf_stress(item)
        output.append(item)
    return output


def _equal_mean_weather_event_days(items: list[dict], source_state: str) -> list[dict]:
    by_date: dict[str, list[dict]] = {}
    for item in items:
        for day in item.get("daily") or []:
            if day.get("date"):
                by_date.setdefault(day["date"], []).append(day)
    continuous_keys = (
        "temperature_mean_c",
        "temperature_min_c",
        "temperature_max_c",
        "night_min_c",
        "dtr_c",
        "precipitation_mm",
        "snowfall_cm",
        "wind_speed_mean_kmh",
    )
    output = []
    for day_date in sorted(by_date):
        rows = by_date[day_date]
        item = {
            "date": day_date,
            "source_state": source_state,
            "complete": bool(rows) and len(rows) == len(items) and all(day.get("complete") for day in rows),
            "available_subregion_count": len(rows),
            "total_subregion_count": len(items),
        }
        for key in continuous_keys:
            values = [metric_value(day, key) for day in rows]
            item[key] = safe_mean([value for value in values if value is not None])
        gust_rows = [
            (metric_value(day, "wind_gust_max_kmh"), day)
            for day in rows
            if metric_value(day, "wind_gust_max_kmh") is not None
        ]
        if gust_rows:
            gust, gust_day = max(gust_rows, key=lambda value: value[0])
            item["wind_gust_max_kmh"] = round(gust, 3)
            item["max_gust_source_point_id"] = gust_day.get("max_gust_source_point_id")
            item["max_gust_source_grid_cell_key"] = gust_day.get("max_gust_source_grid_cell_key")
        else:
            item["wind_gust_max_kmh"] = None
            item["max_gust_source_point_id"] = None
            item["max_gust_source_grid_cell_key"] = None
        event_counts = {}
        for flag in WEATHER_EVENT_FLAG_KEYS:
            triggered = sum(
                int(((day.get("event_counts") or {}).get(flag) or {}).get("triggered_unique_grid_count", 0) or 0)
                for day in rows
            )
            total = sum(
                int(((day.get("event_counts") or {}).get(flag) or {}).get("total_unique_grid_count", 0) or 0)
                for day in rows
            )
            if total == 0:
                # A one-point source row has no nested grid counts only when
                # it came from a malformed fixture; preserve uncertainty.
                values = [day.get(flag) for day in rows if day.get(flag) is not None]
                triggered = sum(value is True for value in values)
                total = len(values)
            event_counts[flag] = {
                "any_grid_event": True if triggered else False if total else None,
                "triggered_unique_grid_count": triggered,
                "total_unique_grid_count": total,
            }
            item[flag] = event_counts[flag]["any_grid_event"]
        item["event_counts"] = event_counts
        item["mechanical_leaf_stress"] = mechanical_leaf_stress(item)
        output.append(item)
    return output


def build_weather_event_source_summary(
    config: dict,
    point_ids: list[str],
    source_items: dict[str, dict],
    *,
    source_state: str,
    minimum_verified_unique_grids: int = 1,
    forecast_date: dt.date | None = None,
) -> dict:
    records = {
        point_id: _weather_event_identity_record(item, point_id)
        for point_id, item in source_items.items()
        if item.get("status") == "OK"
    }
    sampling = grid_sampling_summary(
        config,
        point_ids,
        records,
        minimum_verified_unique_grids=minimum_verified_unique_grids,
    )
    unique_entries = deduplicate_grid_records(list(records.values()))
    daily = _aggregate_weather_event_grid_days(unique_entries, source_state)
    status = "INVALID"
    if unique_entries:
        status = "OK" if sampling.get("status") == "OK" and all(item.get("status") == "OK" for item in source_items.values()) else "PARTIAL"
    result = {
        "status": status,
        "source_state": source_state,
        "point_ids": list(point_ids),
        "daily": daily,
        "metrics": weather_event_window_metrics(daily) if daily else None,
        "sampling": sampling,
        "unique_grid_count": len(unique_entries),
    }
    if forecast_date is not None:
        definitions = {
            item["window"]: item
            for item in history_forward_window_definitions(forecast_date)
        }
        result["windows"] = {
            key: weather_event_window_summary(daily, definition, allow_partial=True)
            for key, definition in definitions.items()
        }
    return result


def build_weather_event_composite(
    subregion_summaries: dict[str, dict],
    subregion_keys: tuple[str, ...],
    *,
    source_state: str,
    forecast_date: dt.date | None = None,
    region_id: str,
) -> dict:
    items = [subregion_summaries[key] for key in subregion_keys if key in subregion_summaries]
    daily = _equal_mean_weather_event_days(items, source_state) if items else []
    statuses = [item.get("status") for item in items]
    if not items:
        status = "INVALID"
    elif all(value == "OK" for value in statuses) and daily:
        status = "OK"
    elif any(value in {"OK", "PARTIAL"} for value in statuses) and daily:
        status = "PARTIAL"
    else:
        status = "INVALID"
    result = {
        "status": status,
        "source_state": source_state,
        "daily": daily,
        "metrics": weather_event_window_metrics(daily) if daily else None,
        "aggregation": f"equal_mean_of_{region_id}_subregions; no point-count weighting; gust=max_over_unique_grids",
        "subregion_statuses": {key: subregion_summaries.get(key, {}).get("status", "INVALID") for key in subregion_keys},
        "missing_or_partial_subregions": [key for key in subregion_keys if subregion_summaries.get(key, {}).get("status") != "OK"],
    }
    if forecast_date is not None:
        definitions = {item["window"]: item for item in history_forward_window_definitions(forecast_date)}
        result["windows"] = {
            key: weather_event_window_summary(daily, definition, allow_partial=True)
            for key, definition in definitions.items()
        }
    return result


def weather_event_point_same_grid_qa(
    config: dict,
    point_id: str,
    year_items: dict[str, dict],
) -> dict:
    configured_years = history_years_for_config(config)
    records = {
        str(year): _weather_event_identity_record(year_items.get(str(year), {}), point_id)
        for year in configured_years
    }
    result = historical_same_grid_qa(records, configured_years)
    year_qa = {}
    failed_years = []
    for year in configured_years:
        item = year_items.get(str(year)) or {}
        identity = item.get("identity") or {}
        qa = item.get("qa") or {}
        distance = qa.get("grid_distance_km", identity.get("grid_distance_km"))
        distance_ok = isinstance(distance, (int, float)) and not isinstance(distance, bool) and distance <= HISTORY_GRID_QA_LIMIT_KM
        item_ok = item.get("status") == "OK" and qa.get("final_status", "PASS") == "PASS" and distance_ok
        year_qa[str(year)] = {
            "status": "PASS" if item_ok else "FAILED",
            "returned_grid_coordinate": identity.get("returned_grid_coordinate"),
            "returned_elevation": identity.get("returned_elevation"),
            "grid_distance_km": distance,
            "grid_distance_limit_km": identity.get("grid_distance_limit_km", HISTORY_GRID_QA_LIMIT_KM),
            "timezone": identity.get("timezone"),
            "model": identity.get("model"),
            "source_history_cache_key": item.get("source_history_cache_key"),
            "api_request_metadata": {
                "endpoint": identity.get("endpoint"),
                "requested_coordinate": identity.get("requested_coordinate"),
                "model_parameter": identity.get("model_parameter"),
                "cell_selection": identity.get("cell_selection"),
                "elevation": identity.get("elevation"),
                "timezone": identity.get("timezone"),
            },
        }
        if not item_ok:
            failed_years.append(str(year))
    result["year_qa"] = year_qa
    if failed_years or result.get("final_status") != "PASS":
        result["final_status"] = "FAILED"
        result["status"] = "FAIL"
        result["reason"] = (
            "WEATHER_EVENT_HISTORY_YEAR_QA_FAILED:" + ",".join(failed_years)
            if failed_years
            else result.get("reason") or "WEATHER_EVENT_HISTORY_GRID_QA_FAILED"
        )
    result["cross_year_comparison_usable"] = result["final_status"] == "PASS"
    return result


def _weather_event_region_source(
    config: dict,
    region_id: str,
    point_ids: list[str],
    source_items: dict[str, dict],
    *,
    source_state: str,
    forecast_date: dt.date | None = None,
    minimum_verified_unique_grids: int = 1,
) -> dict:
    return build_weather_event_source_summary(
        config,
        point_ids,
        source_items,
        source_state=source_state,
        minimum_verified_unique_grids=minimum_verified_unique_grids,
        forecast_date=forecast_date,
    )


def build_weather_event_region(
    config: dict,
    region_id: str,
    historical_items: dict[str, dict[str, dict]],
    forecast_items: dict[str, dict],
    forecast_date: dt.date,
    point_same_grid_qa: dict[str, dict],
) -> dict:
    region_config = config.get("regions", {}).get(region_id, {})
    if region_id in SUBREGION_KEYS_BY_REGION and region_subregion_registry(config, region_id):
        subregions = {}
        for subregion_id in SUBREGION_KEYS_BY_REGION[region_id]:
            registry_item = region_subregion_registry(config, region_id).get(subregion_id) or {}
            point_ids = region_subregion_point_ids(config, region_id, subregion_id)
            verified_ids = region_subregion_point_ids(config, region_id, subregion_id, verified_only=True)
            historical_years = {}
            for year in history_years_for_config(config):
                source_items = {
                    point_id: historical_items.get(point_id, {}).get(str(year), {})
                    for point_id in verified_ids
                    if historical_items.get(point_id, {}).get(str(year))
                }
                historical_years[str(year)] = _weather_event_region_source(
                    config,
                    region_id,
                    point_ids,
                    source_items,
                    source_state="finalized_history",
                    minimum_verified_unique_grids=registry_item.get("minimum_verified_unique_grids", 1),
                )
            forecast_source_items = {
                point_id: forecast_items[point_id]
                for point_id in verified_ids
                if point_id in forecast_items
            }
            forecast = _weather_event_region_source(
                config,
                region_id,
                point_ids,
                forecast_source_items,
                source_state="forecast",
                minimum_verified_unique_grids=registry_item.get("minimum_verified_unique_grids", 1),
                forecast_date=forecast_date,
            )
            subregions[subregion_id] = {
                "name": registry_item.get("name", subregion_id),
                "status": "OK" if forecast.get("status") == "OK" and all(item.get("status") == "OK" for item in historical_years.values()) else "PARTIAL" if forecast.get("status") in {"OK", "PARTIAL"} or any(item.get("status") in {"OK", "PARTIAL"} for item in historical_years.values()) else "INVALID",
                "point_ids": point_ids,
                "verified_point_ids": verified_ids,
                "same_grid_qa": {
                    point_id: copy.deepcopy(point_same_grid_qa.get(point_id))
                    for point_id in verified_ids
                },
                "finalized_history": {"years": historical_years},
                "forecast": forecast,
                "sampling": {
                    "historical_by_year": {year: item.get("sampling") for year, item in historical_years.items()},
                    "forecast": forecast.get("sampling"),
                },
            }
        composite_history = {}
        for year in history_years_for_config(config):
            inputs = {
                subregion_id: subregions[subregion_id]["finalized_history"]["years"][str(year)]
                for subregion_id in SUBREGION_KEYS_BY_REGION[region_id]
                if subregion_id in subregions
            }
            composite_history[str(year)] = build_weather_event_composite(
                inputs,
                SUBREGION_KEYS_BY_REGION[region_id],
                source_state="finalized_history",
                region_id=region_id,
            )
        composite_forecast_inputs = {
            subregion_id: subregions[subregion_id]["forecast"]
            for subregion_id in SUBREGION_KEYS_BY_REGION[region_id]
            if subregion_id in subregions
        }
        composite_forecast = build_weather_event_composite(
            composite_forecast_inputs,
            SUBREGION_KEYS_BY_REGION[region_id],
            source_state="forecast",
            forecast_date=forecast_date,
            region_id=region_id,
        )
        composite = {
            "status": "OK" if composite_forecast.get("status") == "OK" and all(item.get("status") == "OK" for item in composite_history.values()) else "PARTIAL" if composite_forecast.get("status") in {"OK", "PARTIAL"} or any(item.get("status") in {"OK", "PARTIAL"} for item in composite_history.values()) else "INVALID",
            "usable_for_main_chain": composite_forecast.get("windows", {}).get("d0_7", {}).get("status") == "OK",
            "aggregation": f"equal_mean_of_{region_id}_subregions; unique grids equal within subregion; gust=max_over_unique_grids",
            "finalized_history": {"years": composite_history},
            "forecast": composite_forecast,
            "subregion_statuses": {key: subregions.get(key, {}).get("status", "INVALID") for key in SUBREGION_KEYS_BY_REGION[region_id]},
            "missing_or_partial_subregions": [key for key in SUBREGION_KEYS_BY_REGION[region_id] if subregions.get(key, {}).get("status") != "OK"],
        }
        finalized = {"years": composite_history}
        forecast = composite_forecast
        sampling = {key: value.get("sampling") for key, value in subregions.items()}
    else:
        core_id = region_config.get("core_point_id")
        point_ids = [core_id] if core_id else []
        verified_ids = [point_id for point_id in point_ids if point_id in active_points(config)]
        historical_years = {}
        for year in history_years_for_config(config):
            source_items = {
                point_id: historical_items.get(point_id, {}).get(str(year), {})
                for point_id in verified_ids
                if historical_items.get(point_id, {}).get(str(year))
            }
            historical_years[str(year)] = _weather_event_region_source(
                config,
                region_id,
                point_ids,
                source_items,
                source_state="finalized_history",
            )
        forecast_source_items = {
            point_id: forecast_items[point_id]
            for point_id in verified_ids
            if point_id in forecast_items
        }
        forecast = _weather_event_region_source(
            config,
            region_id,
            point_ids,
            forecast_source_items,
            source_state="forecast",
            forecast_date=forecast_date,
        )
        finalized = {"years": historical_years}
        composite = None
        subregions = {}
        sampling = {
            "historical_by_year": {year: item.get("sampling") for year, item in historical_years.items()},
            "forecast": forecast.get("sampling"),
        }
    region_status = "OK" if forecast.get("status") == "OK" and all(item.get("status") == "OK" for item in finalized.get("years", {}).values()) else "PARTIAL" if forecast.get("status") in {"OK", "PARTIAL"} or any(item.get("status") in {"OK", "PARTIAL"} for item in finalized.get("years", {}).values()) else "INVALID"
    finalized_episodes = {
        year: cooling_episode_candidates(item.get("daily") or [])
        for year, item in finalized.get("years", {}).items()
    }
    forecast_episodes = cooling_episode_candidates(forecast.get("daily") or [])
    result = {
        "name": region_config.get("name", region_id),
        "status": region_status,
        "usable_for_main_chain": forecast.get("windows", {}).get("d0_7", {}).get("status") == "OK",
        "sampling": sampling,
        "same_grid_qa": copy.deepcopy(point_same_grid_qa),
        "finalized_history": finalized,
        "forecast": forecast,
        "cooling_episode_candidates": {
            "finalized_history": finalized_episodes,
            "forecast": forecast_episodes,
            "rule_version": COOLING_EPISODE_RULE_VERSION,
        },
        "reason": None if region_status == "OK" else "WEATHER_EVENT_REGION_PARTIAL_OR_INVALID",
    }
    if subregions:
        result["subregions"] = subregions
        result["composite"] = composite
    return result


def run_weather_events(
    config: dict,
    hres: dict,
    generated_at: str,
    data_date: str,
    forecast_date: dt.date,
    *,
    history_cache_dir: Path | None = None,
    cache_dir: Path | None = None,
) -> dict:
    """Build weather events from history cache plus the current HRES result."""
    if history_cache_namespace(config) != "siguniang_jiuzhaigou":
        return module_header(
            "weather_events",
            generated_at,
            data_date,
            "SKIPPED",
            reason="WEATHER_EVENTS_NAMESPACE_ONLY",
            regions={},
        )
    points = active_points(config)
    configured_years = history_years_for_config(config)
    historical_items: dict[str, dict[str, dict]] = {}
    point_same_grid_qa = {}
    cache_stats = weather_event_cache_stats()
    history_successes = 0
    history_failures = 0
    for point_id, point in points.items():
        historical_items[point_id] = {}
        for year in configured_years:
            source_cache, source_info = load_history_cache(config, point, year, history_cache_dir)
            if source_cache is None:
                history_failures += 1
                historical_items[point_id][str(year)] = {
                    "status": "INVALID",
                    "source_state": "finalized_history",
                    "point_id": point_id,
                    "year": year,
                    "identity": {},
                    "qa": {"final_status": "INVALID", "reason": "HISTORY_CACHE_" + source_info.get("status", "MISSING")},
                    "daily": [],
                    "cache_update": {"cache_invalid": 1 if source_info.get("status") == "INVALID" else 0},
                    "source_cache": source_info,
                }
                update_weather_event_cache_stats(cache_stats, historical_items[point_id][str(year)])
                continue
            source_cache["_cache_path"] = source_info.get("path")
            event_cache, update_info = update_weather_events_cache(
                config,
                point,
                year,
                source_cache,
                generated_at,
                cache_dir=cache_dir,
            )
            if event_cache is None:
                history_failures += 1
                historical_items[point_id][str(year)] = {
                    "status": "INVALID",
                    "source_state": "finalized_history",
                    "point_id": point_id,
                    "year": year,
                    "identity": copy.deepcopy(source_cache.get("identity") or {}),
                    "qa": {"final_status": "INVALID", "reason": "WEATHER_EVENT_CACHE_IDENTITY_INVALID"},
                    "daily": [],
                    "cache_update": update_info.get("cache_update", {}),
                    "source_cache": source_info,
                }
            else:
                history_successes += 1
                item = _weather_event_item_from_history_cache(
                    config,
                    point,
                    year,
                    {
                        **event_cache,
                        "record_metadata": source_cache.get("record_metadata", {}),
                    },
                    update_info.get("cache_update", {}),
                )
                item["source_history_cache_key"] = event_cache.get("source_history_cache_key")
                item["source_history_cache_path"] = source_info.get("path")
                item["cache_path"] = update_info.get("path")
                item["cache_update"] = update_info.get("cache_update", {})
                historical_items[point_id][str(year)] = item
            update_weather_event_cache_stats(cache_stats, historical_items[point_id][str(year)])
        point_same_grid_qa[point_id] = weather_event_point_same_grid_qa(
            config,
            point_id,
            historical_items[point_id],
        )
    forecast_items = {}
    forecast_successes = 0
    forecast_failures = 0
    cutoff = WEATHER_EVENTS_CUTOFF
    for point_id, point in points.items():
        record = (hres.get("points") or {}).get(point_id)
        if record and record.get("status") == "PASS":
            item = _weather_event_item_from_forecast_record(point, record, cutoff)
        else:
            item = {
                "status": "INVALID",
                "source_state": "forecast",
                "point_id": point_id,
                "identity": {},
                "qa": {"final_status": "INVALID", "reason": "HRES_POINT_INVALID"},
                "daily": [],
                "metrics": None,
            }
        forecast_items[point_id] = item
        if item.get("status") == "OK":
            forecast_successes += 1
        else:
            forecast_failures += 1
    regions = {
        region_id: build_weather_event_region(
            config,
            region_id,
            historical_items,
            forecast_items,
            forecast_date,
            point_same_grid_qa,
        )
        for region_id in CORE_REGION_IDS
    }
    status = "OK" if history_failures == 0 and forecast_failures == 0 and all(item.get("status") == "OK" for item in regions.values()) else "PARTIAL" if history_successes or forecast_successes else "FAILED"
    finalized_points = {
        point_id: {
            "point_id": point_id,
            "point": {"name": point.get("name"), "region": point.get("region"), "status": point.get("status")},
            "same_grid_qa": point_same_grid_qa[point_id],
            "years": historical_items[point_id],
        }
        for point_id, point in points.items()
    }
    forecast_points = {
        point_id: {
            key: copy.deepcopy(value)
            for key, value in item.items()
            if key not in {"cache_update"}
        }
        for point_id, item in forecast_items.items()
    }
    cooling = {
        "finalized_history": {
            region_id: copy.deepcopy((region.get("cooling_episode_candidates") or {}).get("finalized_history", {}))
            for region_id, region in regions.items()
        },
        "forecast": {
            region_id: copy.deepcopy((region.get("cooling_episode_candidates") or {}).get("forecast", []))
            for region_id, region in regions.items()
        },
        "rule_version": COOLING_EPISODE_RULE_VERSION,
    }
    return module_header(
        "weather_events",
        generated_at,
        data_date,
        status,
        source="Open-Meteo",
        finalized_history={
            "source_state": "finalized_history",
            "source": "Open-Meteo Historical Weather API via history cache",
            "model": HISTORY_MODEL,
            "model_parameter": HISTORY_MODEL_PARAMETER,
            "history_years": list(configured_years),
            "points": finalized_points,
        },
        forecast={
            "source_state": "forecast",
            "source": "Open-Meteo",
            "endpoint": OPEN_METEO_ENDPOINTS["hres"],
            "model": "ECMWF IFS HRES 9 km",
            "forecast_date": forecast_date.isoformat(),
            "cutoff_date": cutoff.isoformat(),
            "points": forecast_points,
            "historical_promotion_allowed": False,
        },
        regions=regions,
        cooling_episode_candidates=cooling,
        weather_event_cache={
            "enabled": True,
            "directory": history_cache_relative_path(Path(cache_dir) if cache_dir is not None else WEATHER_EVENTS_CACHE_DIR),
            "source_history_cache_directory": history_cache_relative_path(Path(history_cache_dir) if history_cache_dir is not None else HISTORY_CACHE_DIR),
            "historical_api_requests": 0,
            **cache_stats,
        },
        qa={
            "final_status": "PASS" if status == "OK" else "PARTIAL" if status == "PARTIAL" else "FAILED",
            "history_source_records": {"successful": history_successes, "failed": history_failures, "expected": len(points) * len(configured_years)},
            "forecast_records": {"successful": forecast_successes, "failed": forecast_failures, "expected": len(points)},
            "forecast_cutoff_date": cutoff.isoformat(),
            "forecast_dates_after_cutoff_emitted": False,
            "historical_source_is_cache_only": True,
        },
        interpretation_boundary="Weather events and transparent heuristics only; no ecological or travel conclusion is generated.",
        excluded_points=excluded_points(config),
        successful_points=forecast_successes,
        failed_points=history_failures + forecast_failures,
    )


def numeric_deltas(current: dict, baseline: dict) -> dict:
    delta = {}
    for key, value in current.items():
        base = baseline.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and isinstance(base, (int, float)) and not isinstance(base, bool):
            delta[key] = round(value - base, 3)
        elif isinstance(value, dict) and isinstance(base, dict):
            nested = numeric_deltas(value, base)
            if nested:
                delta[key] = nested
    return delta


def driver_direction(current: dict, baseline: dict, baseline_year: int = 2025) -> dict:
    current_index = current.get("coldness_index")
    baseline_index = baseline.get("coldness_index")
    current_counts = current.get("threshold_nights", {})
    baseline_counts = baseline.get("threshold_nights", {})
    count_delta = sum(
        int(current_counts.get(key, 0)) - int(baseline_counts.get(key, 0))
        for key in ("below_10_c", "below_5_c", "below_2_c", "below_0_c")
    )
    if current.get("days_available", 0) < 3 or baseline.get("days_available", 0) < 3 or current_index is None or baseline_index is None:
        direction = "UNDETERMINED"
        strength = "WEAK"
    else:
        delta = float(current_index) - float(baseline_index)
        if delta >= 5 or count_delta >= 2:
            direction = "LEADING"
        elif delta <= -5 or count_delta <= -2:
            direction = "LAGGING"
        else:
            direction = "SYNC"
        magnitude = abs(delta)
        strong_cutoff = max(10.0, abs(float(baseline_index)) * 0.25)
        moderate_cutoff = max(5.0, abs(float(baseline_index)) * 0.1)
        strength = "STRONG" if magnitude >= strong_cutoff else "MODERATE" if magnitude >= moderate_cutoff else "WEAK"
    return {
        "direction": direction,
        "strength": strength,
        "evidence": {
            "current_2026": current,
            f"baseline_{baseline_year}": baseline,
            f"delta_2026_minus_{baseline_year}": numeric_deltas(current, baseline),
            "threshold_count_delta_sum": count_delta,
            "interpretation": "weather_driver_only; an actual phenology assessment still needs external visual evidence (photos)",
        },
    }


def undetermined_weather_driver(reason: str) -> dict:
    return {
        "direction": "UNDETERMINED",
        "strength": "WEAK",
        "evidence": {"reason": reason},
    }


def historical_same_grid_qa(years: dict[str, dict], configured_years: tuple[int, ...]) -> dict:
    """Require every configured historical year to use one returned model grid."""
    year_keys = [str(year) for year in configured_years]
    grids = {
        year: (years.get(year, {}).get("response") or {}).get("grid_coordinate")
        for year in year_keys
    }
    requested_coordinates = {
        year: (years.get(year, {}).get("request") or {}).get("coordinate")
        for year in year_keys
    }
    pairwise = {}
    for index, left in enumerate(year_keys):
        for right in year_keys[index + 1:]:
            if grids[left] and grids[right]:
                pairwise[f"{left}_vs_{right}"] = "PASS" if grids[left] == grids[right] else "FAIL"
            else:
                pairwise[f"{left}_vs_{right}"] = "UNAVAILABLE"
    available_grids = [grids[year] for year in year_keys if grids[year]]
    if len(available_grids) != len(year_keys):
        grid_status = "UNAVAILABLE"
    else:
        grid_status = "PASS" if len({json.dumps(grid, sort_keys=True) for grid in available_grids}) == 1 else "FAIL"
    available_coordinates = [requested_coordinates[year] for year in year_keys if requested_coordinates[year]]
    if len(available_coordinates) != len(year_keys):
        coordinate_status = "UNAVAILABLE"
    else:
        coordinate_status = "PASS" if len({json.dumps(coordinate, sort_keys=True) for coordinate in available_coordinates}) == 1 else "FAIL"
    final_status = "PASS" if grid_status == "PASS" and coordinate_status == "PASS" else "FAILED"
    reason = None
    if grid_status == "FAIL":
        reason = "HISTORICAL_GRID_MISMATCH"
    elif coordinate_status == "FAIL":
        reason = "HISTORICAL_REQUEST_COORDINATE_MISMATCH"
    elif final_status == "FAILED":
        reason = "HISTORICAL_GRID_OR_COORDINATE_UNAVAILABLE"
    result = {
        "status": grid_status,
        "final_status": final_status,
        "checked_years": year_keys,
        "returned_grids": grids,
        "pairwise": pairwise,
        "same_requested_coordinate": coordinate_status,
        "requested_coordinates": requested_coordinates,
        "reason": reason,
    }
    # Preserve the v1.0/v1.1 pairwise fields for existing machine-readable readers.
    for year in ("2025", "2026"):
        result[f"returned_grid_{year}"] = grids.get(year)
    return result


def build_history_comparison(config: dict, point_results: dict[str, dict], data_date: str) -> dict:
    configured_years = history_years_for_config(config)
    current_year = 2026
    comparisons: dict[str, dict] = {}
    for point_id, result in point_results.items():
        years = result["years"]
        daily_by_year = {
            str(year): (
                years.get(str(year), {}).get("daily", [])
                if years.get(str(year), {}).get("status") == "PASS"
                else []
            )
            for year in configured_years
        }
        metrics_by_year = {
            year: period_metrics(days)
            for year, days in daily_by_year.items()
        }
        same_grid_qa = historical_same_grid_qa(years, configured_years)
        history_start = result.get("history_start_month_day")
        if not history_start:
            history_start = config.get("regions", {}).get(result["point"]["region"], {}).get(
                "history_start_month_day",
                config.get("history_start_month_day", "08-25"),
            )
        complete_years = all(
            years.get(str(year), {}).get("status") == "PASS" and daily_by_year[str(year)]
            for year in configured_years
        )
        comparison_status = (
            "OK"
            if complete_years and same_grid_qa["final_status"] == "PASS"
            else "FAILED"
        )
        metrics_2026 = metrics_by_year.get(str(current_year), {})
        comparison_deltas = {
            str(year): numeric_deltas(metrics_2026, metrics_by_year[str(year)])
            for year in configured_years
            if year != current_year and comparison_status == "OK"
        }
        comparison = {
            "point_id": point_id,
            "region": result["point"]["region"],
            "status": comparison_status,
            "period_start": history_start,
            "period_end": data_date,
            "daily": daily_by_year,
            "metrics": metrics_by_year,
            "deltas_2026_minus": comparison_deltas,
            "delta_2026_minus_2023": comparison_deltas.get("2023", {}),
            "delta_2026_minus_2024": comparison_deltas.get("2024", {}),
            "delta_2026_minus_2025": comparison_deltas.get("2025", {}),
            "same_grid_qa": same_grid_qa,
        }
        for year in configured_years:
            if year != current_year:
                comparison[f"weather_driver_vs_{year}"] = driver_direction(
                    metrics_2026,
                    metrics_by_year.get(str(year), {}),
                    baseline_year=year,
                )
        comparisons[point_id] = comparison
    region_summaries: dict[str, dict] = {}
    for region_id in core_region_ids(config):
        region_config = config["regions"][region_id]
        core_id = region_config.get("core_point_id")
        comparison = comparisons.get(core_id) if core_id else None
        if comparison:
            region_summary = {
                "region": region_id,
                "core_point_id": core_id,
                "status": comparison["status"],
                "usable_for_main_chain": True,
                "metrics": comparison["metrics"],
                "supporting_point_ids": [point_id for point_id, item in comparisons.items() if item["region"] == region_id and point_id != core_id],
            }
            region_summary["visit_date"] = region_config.get("primary_visit_date")
            for year in configured_years:
                if year != current_year:
                    region_summary[f"weather_driver_vs_{year}"] = comparison[f"weather_driver_vs_{year}"]
            region_summaries[region_id] = region_summary
    return {"points": comparisons, "regions": region_summaries}


def run_gfs(config: dict, client: ApiClient, generated_at: str, data_date: str) -> dict:
    points = active_points(config)
    records = {}
    for point_id, point in points.items():
        records[point_id] = fetch_point(
            client,
            point=point,
            source="Open-Meteo",
            endpoint=OPEN_METEO_ENDPOINTS["gfs"],
            model="NCEP GFS Global 0.11°",
            params=base_weather_params(point, forecast_days=16),
            variables=GFS_VARIABLES,
            required_variables=GFS_REQUIRED_VARIABLES,
            optional_variables=GFS_OPTIONAL_VARIABLES,
            grid_limit_km=19.5,
            log_label=f"{point_id}:GFS",
            precision_module="gfs",
            max_forecast_date=point_forecast_end_date(point),
        )
    values = list(records.values())
    variable_status = aggregate_variable_status(
        [record.get("variable_status") or {} for record in values]
    )
    unavailable = unavailable_variables_for(
        variable_status,
        required_variables=GFS_REQUIRED_VARIABLES,
        optional_variables=GFS_OPTIONAL_VARIABLES,
    )
    return module_header(
        "gfs",
        generated_at,
        data_date,
        module_status(values, len(points)),
        endpoint=OPEN_METEO_ENDPOINTS["gfs"],
        model="NCEP GFS Global 0.11°",
        native_resolution="0.11° (~13 km)",
        precision_policy=PRECISION_POLICIES["gfs"],
        interpolation_note="Open-Meteo documents GFS as hourly, with 3-hourly native data interpolated after 120 h.",
        requested_variables=list(GFS_VARIABLES),
        required_variables=list(GFS_REQUIRED_VARIABLES),
        optional_variables=list(GFS_OPTIONAL_VARIABLES),
        variable_status=variable_status,
        **unavailable,
        points=records,
        excluded_points=excluded_points(config),
        successful_points=sum(record.get("status") == "PASS" for record in values),
        failed_points=sum(record.get("status") != "PASS" for record in values),
    )


SAMPLE_BEARINGS_DEGREES = {
    "N": 0,
    "S": 180,
    "E": 90,
    "W": 270,
    "NE": 45,
    "NW": 315,
    "SE": 135,
    "SW": 225,
}


def offset_coordinate(latitude: float, longitude: float, distance_km: float, direction: str) -> tuple[float, float]:
    bearing = math.radians(SAMPLE_BEARINGS_DEGREES[direction])
    latitude_delta = distance_km * math.cos(bearing) / 111.32
    longitude_delta = distance_km * math.sin(bearing) / (111.32 * math.cos(math.radians(latitude)))
    return round(latitude + latitude_delta, 6), round(longitude + longitude_delta, 6)


def sample_definitions(region_id: str, core_point: dict, radius_km: float, directions: list[str]) -> list[dict]:
    samples = [{
        "sample_id": f"{region_id}:CORE",
        "direction": "CORE",
        "requested_coordinate": {"latitude": core_point["latitude"], "longitude": core_point["longitude"]},
    }]
    for direction in directions:
        latitude, longitude = offset_coordinate(core_point["latitude"], core_point["longitude"], radius_km, direction)
        samples.append({
            "sample_id": f"{region_id}:{direction}",
            "direction": direction,
            "requested_coordinate": {"latitude": latitude, "longitude": longitude},
        })
    return samples


def record_grid_cell_key(record: dict) -> str | None:
    coordinate = (record.get("response") or {}).get("grid_coordinate") or {}
    if not valid_coordinate(coordinate.get("latitude"), coordinate.get("longitude")):
        return None
    return f"{float(coordinate['latitude']):.6f},{float(coordinate['longitude']):.6f}"


def compact_spatial_sample(sample: dict, record: dict) -> dict:
    response = record.get("response") or {}
    return {
        "sample_id": sample["sample_id"],
        "direction": sample["direction"],
        "requested_coordinate": sample["requested_coordinate"],
        "returned_grid_coordinate": response.get("grid_coordinate"),
        "returned_elevation": response.get("returned_elevation"),
        "grid_distance_km": (record.get("qa") or {}).get("grid_distance_km"),
        "grid_cell_key": record_grid_cell_key(record),
        "status": record.get("status"),
        "source": record.get("source"),
        "endpoint": record.get("endpoint"),
        "model": record.get("model"),
        "qa": record.get("qa"),
        "daily": record.get("daily", []),
    }


def spatial_region_summary(region_id: str, samples: list[dict]) -> dict:
    valid_samples = [sample for sample in samples if sample.get("status") == "PASS" and sample.get("grid_cell_key")]
    by_cell: dict[str, dict] = {}
    for sample in valid_samples:
        by_cell.setdefault(sample["grid_cell_key"], sample)
    daily_by_cell: dict[str, dict[str, dict]] = {
        cell: {item["date"]: item for item in sample.get("daily", [])}
        for cell, sample in by_cell.items()
    }
    dates = sorted({date for values in daily_by_cell.values() for date in values})[:7]
    temperatures = []
    for sample in by_cell.values():
        for day in sample.get("daily", [])[:7]:
            for key in ("temperature_min_c", "temperature_max_c"):
                value = metric_value(day, key)
                if value is not None:
                    temperatures.append(value)
    coverage_by_date = []
    for date in dates:
        cells_with_data = [values[date] for values in daily_by_cell.values() if date in values]
        cold_cells = [day for day in cells_with_data if metric_value(day, "night_min_c") is not None and metric_value(day, "night_min_c") < 5]
        total_cells = len(cells_with_data)
        ratio = len(cold_cells) / total_cells if total_cells else None
        label = "undetermined" if ratio is None else "widespread" if ratio >= 0.75 else "mixed" if ratio >= 0.5 else "localized" if ratio > 0 else "none"
        coverage_by_date.append({
            "date": date,
            "threshold": "night_min_c < 5",
            "cold_cells": len(cold_cells),
            "total_cells": total_cells,
            "coverage_ratio": round(ratio, 3) if ratio is not None else None,
            "label": label,
        })
    cells_with_data = set(daily_by_cell)
    cold_cells = {
        cell
        for cell, values in daily_by_cell.items()
        if any(
            metric_value(day, "night_min_c") is not None and metric_value(day, "night_min_c") < 5
            for day in values.values()
        )
    }
    total_cell_observations = sum(item["total_cells"] for item in coverage_by_date)
    cold_cell_observations = sum(item["cold_cells"] for item in coverage_by_date)
    daily_ratio = cold_cell_observations / total_cell_observations if total_cell_observations else None
    unique_ratio = len(cold_cells) / len(cells_with_data) if cells_with_data else None
    any_label = "undetermined" if unique_ratio is None else "widespread" if unique_ratio >= 0.75 else "mixed" if unique_ratio >= 0.5 else "localized" if unique_ratio > 0 else "none"
    return {
        "requested_samples": len(samples),
        "valid_samples": len(valid_samples),
        "failed_samples": len(samples) - len(valid_samples),
        "unique_model_cells": len(by_cell),
        "duplicate_requested_samples": len(valid_samples) - len(by_cell),
        "temperature_range_c": {
            "min": round(min(temperatures), 3) if temperatures else None,
            "max": round(max(temperatures), 3) if temperatures else None,
            "window_days": len(dates),
        },
        "cold_pool_coverage": {
            "threshold": "night_min_c < 5",
            "next_7d": {
                "cold_cells": len(cold_cells),
                "total_cells": len(cells_with_data),
                "cold_cell_observations": cold_cell_observations,
                "cell_observations": total_cell_observations,
                "coverage_ratio": round(daily_ratio, 3) if daily_ratio is not None else None,
                "unique_cell_coverage_ratio": round(unique_ratio, 3) if unique_ratio is not None else None,
                "label": any_label,
            },
            "by_date": coverage_by_date,
        },
    }


def run_spatial(config: dict, client: ApiClient, generated_at: str, data_date: str, hres: dict) -> dict:
    regions: dict[str, dict] = {}
    enabled_region_statuses = []
    points = active_points(config)
    for region_id, region_config in config["regions"].items():
        core_id = region_config.get("core_point_id")
        core_point = points.get(core_id) if core_id else None
        if not core_point:
            log(f"[{region_id}] SKIPPED: PROVISIONAL")
            regions[region_id] = {
                "status": "SKIPPED",
                "usable_for_main_chain": False,
                "reason": "NO_VERIFIED_CORE_POINT",
                "requested_samples": 0,
                "unique_model_cells": 0,
                "samples": [],
            }
            continue
        samples = sample_definitions(
            region_id,
            core_point,
            float(config["sampling"]["radius_km"]),
            list(config["sampling"]["directions"]),
        )
        full_records = []
        compact_samples = []
        sampling_grid_limit_km = float(config["sampling"]["radius_km"]) + float(config["sampling"].get("grid_qa_extra_allowance_km", 4.5))
        for sample in samples:
            if sample["direction"] == "CORE":
                seeded = hres.get("points", {}).get(core_id)
                if seeded and seeded.get("status") == "PASS":
                    record = copy.deepcopy(seeded)
                else:
                    record = None
            else:
                record = None
            if record is None:
                sample_point = {
                    "id": sample["sample_id"],
                    "name": f"{region_config['name']} {sample['direction']} sample",
                    "region": region_id,
                    "status": "VERIFIED",
                    **sample["requested_coordinate"],
                }
                record = fetch_point(
                    client,
                    point=sample_point,
                    source="Open-Meteo",
                    endpoint=OPEN_METEO_ENDPOINTS["hres"],
                    model="ECMWF IFS HRES 9 km",
                    params=base_weather_params(sample_point, forecast_days=7),
                    variables=["temperature_2m", "precipitation", "snowfall", "wind_gusts_10m"],
                    required_variables=["temperature_2m", "precipitation", "snowfall", "wind_gusts_10m"],
                    grid_limit_km=sampling_grid_limit_km,
                    log_label=f"{sample['sample_id']}:SPATIAL",
                    precision_module="hres",
                    max_forecast_date=point_forecast_end_date(core_point),
                )
            full_records.append(record)
            compact_samples.append(compact_spatial_sample(sample, record))
        region_summary = spatial_region_summary(region_id, compact_samples)
        region_status = "OK" if region_summary["failed_samples"] == 0 else "FAILED"
        enabled_region_statuses.append(region_status)
        regions[region_id] = {
            "status": region_status,
            "usable_for_main_chain": True,
            "core_point_id": core_id,
            "analysis_window": {
                "start": next((item["date"] for sample in compact_samples for item in sample.get("daily", [])), None),
                "days": 7,
            },
            **region_summary,
            "samples": compact_samples,
        }
    status = "OK" if enabled_region_statuses and all(value == "OK" for value in enabled_region_statuses) else "FAILED"
    return module_header(
        "spatial_sampling",
        generated_at,
        data_date,
        status,
        endpoint=OPEN_METEO_ENDPOINTS["hres"],
        model="ECMWF IFS HRES 9 km",
        sampling_config=config["sampling"],
        regions=regions,
        excluded_points=excluded_points(config),
    )


def percentile(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return round(ordered[lower], 3)
    fraction = position - lower
    return round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction, 3)


def ensemble_statistics(values: list[float]) -> dict:
    p25 = percentile(values, 0.25)
    p75 = percentile(values, 0.75)
    return {
        "mean": round(mean(values), 3) if values else None,
        "median": round(median(values), 3) if values else None,
        "p10": percentile(values, 0.10),
        "p25": p25,
        "p75": p75,
        "p90": percentile(values, 0.90),
        "interquartile_spread": round(p75 - p25, 3) if p25 is not None and p75 is not None else None,
        "spread": round(percentile(values, 0.90) - percentile(values, 0.10), 3)
        if values
        else None,
    }


def member_series_keys(hourly: dict, variable: str) -> list[str]:
    member_keys = sorted(
        (key for key in hourly if re.fullmatch(re.escape(variable) + r"_member\d{2}", key)),
        key=lambda key: int(key.rsplit("member", 1)[1]),
    )
    return member_keys


def ensemble_series_keys(hourly: dict, variable: str) -> list[str]:
    return [variable, *member_series_keys(hourly, variable)]


def ensemble_daily_distributions(hourly: dict) -> dict:
    times = hourly.get("time") or []
    groups: dict[str, list[int]] = {}
    for index, value in enumerate(times):
        groups.setdefault(parse_local_api_time(value).date().isoformat(), []).append(index)
    temperature_keys = ensemble_series_keys(hourly, "temperature_2m")
    output = {"night_min": [], "daily_mean": [], "variables": [], "probabilities": []}
    for day, indices in sorted(groups.items()):
        night_indices = [index for index in indices if parse_local_api_time(times[index]).hour <= 6 or parse_local_api_time(times[index]).hour >= 20]
        night_values = []
        daily_means = []
        for key in temperature_keys:
            night = _values_for_indices(hourly, key, night_indices)
            day_values = _values_for_indices(hourly, key, indices)
            if night:
                night_values.append(min(night))
            if day_values:
                daily_means.append(mean(day_values))
        night_stats = ensemble_statistics(night_values)
        daily_stats = ensemble_statistics(daily_means)
        thresholds = {}
        for threshold in THRESHOLDS_C:
            key = f"below_{str(int(threshold))}_c"
            below = sum(value < threshold for value in night_values)
            thresholds[key] = {
                "members": below,
                "total_members": len(temperature_keys),
                "probability": round(below / len(temperature_keys), 3) if temperature_keys else None,
            }
        output["night_min"].append({"date": day, "statistics_c": night_stats, "thresholds": thresholds})
        output["daily_mean"].append({"date": day, "statistics_c": daily_stats})
        member_values = []
        for key in temperature_keys:
            item = _gefs_member_aggregate(hourly, key[len("temperature_2m"):], indices)
            if item:
                member_values.append(item)
        members_valid = len(temperature_keys)
        output["variables"].append({
            "date": day,
            "members_valid": members_valid,
            "statistics": _gefs_distribution_summary(member_values, members_valid),
        })
        output["probabilities"].append({
            "date": day,
            "members_valid": members_valid,
            "probabilities": (_gefs_distribution_summary(member_values, members_valid) or {}).get("probabilities", {}),
        })
    return output


def validate_ensemble_members(record: dict) -> tuple[bool, dict]:
    """Validate the 51-series ECMWF ensemble with an explicit required/optional split.

    An unavailable optional variable is reported, never substituted, and never
    allowed to invalidate the whole ECMWF ensemble module.
    """
    hourly = record.get("hourly") or {}
    times = hourly.get("time") if isinstance(hourly.get("time"), list) else []
    series_by_variable = {}
    required_missing = []
    optional_missing = []
    length_mismatch = []
    null_values = []
    required_length_mismatch = []
    required_null_values = []
    required_names = set(EC_ENSEMBLE_REQUIRED_VARIABLES)
    for variable in ENSEMBLE_VARIABLES:
        keys = ensemble_series_keys(hourly, variable)
        series_by_variable[variable] = keys
        required = variable in required_names
        if len(keys) != EC_ENSEMBLE_TOTAL_MEMBERS:
            entry = f"{variable}:expected_{EC_ENSEMBLE_TOTAL_MEMBERS}_got_{len(keys)}"
            (required_missing if required else optional_missing).append(entry)
        for key in keys:
            if len(hourly.get(key, [])) != len(times):
                length_mismatch.append(key)
                if required:
                    required_length_mismatch.append(key)
            if any(value is None for value in hourly.get(key, [])):
                null_values.append(key)
                if required:
                    required_null_values.append(key)
    required_issues = required_missing or required_length_mismatch or required_null_values
    valid = not required_issues
    return valid, {
        "status": "PASS" if valid else "FAIL",
        "expected_members": EC_ENSEMBLE_TOTAL_MEMBERS,
        "series_by_variable": series_by_variable,
        "missing_or_wrong_count": required_missing + optional_missing,
        "required_missing_or_wrong_count": required_missing,
        "optional_missing_or_wrong_count": optional_missing,
        "array_length_mismatch": length_mismatch,
        "null_data_series": null_values,
        "required_array_length_mismatch": required_length_mismatch,
        "required_null_data_series": required_null_values,
        "required_variables": list(EC_ENSEMBLE_REQUIRED_VARIABLES),
        "optional_variables": list(EC_ENSEMBLE_OPTIONAL_VARIABLES),
    }


def run_ensemble(config: dict, client: ApiClient, generated_at: str, data_date: str) -> dict:
    points = active_points(config)
    records: dict[str, dict] = {}
    active_core_ids = []
    for region_id in core_region_ids(config):
        core_id = config["regions"][region_id].get("core_point_id")
        point = points.get(core_id) if core_id else None
        if not point:
            log(f"[{region_id}] ENSEMBLE SKIPPED: PROVISIONAL")
            continue
        active_core_ids.append(core_id)
        record = fetch_point(
            client,
            point=point,
            source="Open-Meteo",
            endpoint=OPEN_METEO_ENDPOINTS["ensemble"],
            model="ECMWF IFS 0.25° Ensemble (51 members)",
            params=base_weather_params(
                point,
                models="ecmwf_ifs025_ensemble",
                forecast_days=ECMWF_ENSEMBLE_FORECAST_DAYS,
            ),
            variables=ENSEMBLE_VARIABLES,
            required_variables=EC_ENSEMBLE_REQUIRED_VARIABLES,
            optional_variables=EC_ENSEMBLE_OPTIONAL_VARIABLES,
            grid_limit_km=37.5,
            log_label=f"{core_id}:ENSEMBLE",
            max_forecast_date=point_forecast_end_date(point),
        )
        if record.get("status") == "PASS":
            members_valid, member_check = validate_ensemble_members(record)
            record["qa"]["ensemble_member_check"] = member_check
            if not members_valid:
                record["status"] = "INVALID"
                record["qa"]["valid"] = False
                record["qa"]["final_status"] = "INVALID"
                record["qa"]["reason"] = "ENSEMBLE_MEMBER_SCHEMA_INVALID"
                log(f"[{core_id}] ENSEMBLE MEMBER QA FAIL")
            else:
                record["ensemble"] = {
                    "model_id": "ecmwf_ifs025_ensemble",
                    "resolution": "0.25° (~25 km)",
                    "total_members": EC_ENSEMBLE_TOTAL_MEMBERS,
                    "requested_variables": list(ENSEMBLE_VARIABLES),
                    "required_variables": list(EC_ENSEMBLE_REQUIRED_VARIABLES),
                    "optional_variables": list(EC_ENSEMBLE_OPTIONAL_VARIABLES),
                    "distribution_variables": list(ENSEMBLE_DISTRIBUTION_VARIABLES),
                    "variable_status": record.get("variable_status") or {},
                    "unavailable_variables": record.get("unavailable_variables") or [],
                    "optional_unavailable_variables": record.get("optional_unavailable_variables") or [],
                    "distributions": ensemble_daily_distributions(record["hourly"]),
                }
        records[core_id] = record
    values = list(records.values())
    variable_status = aggregate_variable_status(
        [(record.get("ensemble") or {}).get("variable_status") or {} for record in values]
    )
    unavailable = unavailable_variables_for(
        variable_status,
        required_variables=EC_ENSEMBLE_REQUIRED_VARIABLES,
        optional_variables=EC_ENSEMBLE_OPTIONAL_VARIABLES,
    )
    return module_header(
        "ensemble",
        generated_at,
        data_date,
        module_status(values, len(active_core_ids)),
        endpoint=OPEN_METEO_ENDPOINTS["ensemble"],
        model="ECMWF IFS 0.25° Ensemble",
        model_id="ecmwf_ifs025_ensemble",
        resolution="0.25° (~25 km)",
        total_members=EC_ENSEMBLE_TOTAL_MEMBERS,
        requested_forecast_days=ECMWF_ENSEMBLE_FORECAST_DAYS,
        requested_variables=list(ENSEMBLE_VARIABLES),
        required_variables=list(EC_ENSEMBLE_REQUIRED_VARIABLES),
        optional_variables=list(EC_ENSEMBLE_OPTIONAL_VARIABLES),
        distribution_variables=list(ENSEMBLE_DISTRIBUTION_VARIABLES),
        variable_status=variable_status,
        **unavailable,
        notes=[
            "Global 51-member ECMWF IFS ensemble is used for western Sichuan.",
            "Ensemble spread is signal robustness, not point-level temperature precision.",
            "Wind direction is stored hourly and summarised with member vector means only.",
        ],
        points=records,
        excluded_points=excluded_points(config),
        successful_points=sum(record.get("status") == "PASS" for record in values),
        failed_points=sum(record.get("status") != "PASS" for record in values),
    )


def _gefs_member_suffixes(hourly: dict, variable: str) -> list[str]:
    """Return the control/member suffixes actually returned for one variable."""
    if not isinstance(hourly.get(variable), list):
        return []
    keys = [key for key in ensemble_series_keys(hourly, variable) if key in hourly]
    return [key.removeprefix(variable) for key in keys]


def _gefs_member_id(suffix: str) -> str:
    return "control" if not suffix else suffix.removeprefix("_")


def _gefs_value_key(variable: str, suffix: str) -> str:
    return f"{variable}{suffix}"


def _gefs_member_check(
    hourly: dict,
    *,
    variables=None,
    required_variables=None,
    optional_variables=None,
    expected_members: int | None = None,
) -> tuple[bool, dict]:
    """Validate ensemble members with an explicit required/optional boundary.

    Shared by the independent GEFS chain and the ECMWF ensemble so both model
    families report member availability the same way.  Defaults describe GEFS.
    """
    variables = list(variables if variables is not None else GEFS_CORE_VARIABLES)
    required_variables = tuple(
        required_variables if required_variables is not None else GEFS_REQUIRED_VARIABLES
    )
    optional_variables = tuple(
        optional_variables if optional_variables is not None else GEFS_OPTIONAL_VARIABLES
    )
    expected_members = (
        expected_members if expected_members is not None else GEFS_ENSEMBLE_MEMBERS
    )
    times = hourly.get("time") if isinstance(hourly.get("time"), list) else []
    variable_counts = {
        variable: len(_gefs_member_suffixes(hourly, variable))
        for variable in variables
    }
    present_variables = [variable for variable, count in variable_counts.items() if count]
    candidate_suffixes = set(_gefs_member_suffixes(hourly, "temperature_2m"))
    if not candidate_suffixes:
        candidate_suffixes = set(
            suffix
            for variable in present_variables
            for suffix in _gefs_member_suffixes(hourly, variable)
        )
    valid_by_variable: dict[str, list[str]] = {}
    array_length_mismatch = []
    null_data_series = []
    partial_data_series = []
    for variable in present_variables:
        valid_suffixes = []
        for suffix in _gefs_member_suffixes(hourly, variable):
            values = hourly.get(_gefs_value_key(variable, suffix))
            if not isinstance(values, list) or len(values) != len(times):
                array_length_mismatch.append(_gefs_value_key(variable, suffix))
                continue
            if all(value is None for value in values):
                null_data_series.append(_gefs_value_key(variable, suffix))
                continue
            if any(value is None for value in values):
                partial_data_series.append(_gefs_value_key(variable, suffix))
            valid_suffixes.append(suffix)
        valid_by_variable[variable] = valid_suffixes
    required_names = set(required_variables)
    optional_names = set(optional_variables)
    unavailable_variables = [
        variable for variable in present_variables
        if not valid_by_variable.get(variable)
    ]
    required_missing_variables = sorted(
        variable for variable in required_variables
        if variable_counts.get(variable, 0) == 0 or not valid_by_variable.get(variable)
    )
    optional_missing_variables = sorted(
        variable for variable in optional_variables
        if variable in variables
        and (variable_counts.get(variable, 0) == 0 or not valid_by_variable.get(variable))
    )
    required_array_length_mismatch = sorted(
        value for value in array_length_mismatch
        if value.split("_member", 1)[0] in required_names
    )
    optional_array_length_mismatch = sorted(
        value for value in array_length_mismatch
        if value.split("_member", 1)[0] in optional_names
    )
    required_null_data_series = sorted(
        value for value in null_data_series
        if value.split("_member", 1)[0] in required_names
    )
    optional_null_data_series = sorted(
        value for value in null_data_series
        if value.split("_member", 1)[0] in optional_names
    )
    required_partial_data_series = sorted(
        value for value in partial_data_series
        if value.split("_member", 1)[0] in required_names
    )
    optional_partial_data_series = sorted(
        value for value in partial_data_series
        if value.split("_member", 1)[0] in optional_names
    )
    # Optional variables must never reduce the common member denominator.
    # Required variables that are missing are excluded from the intersection
    # but remain visible in required_missing_variables and force PARTIAL.
    available_required = [
        variable for variable in required_variables
        if valid_by_variable.get(variable)
    ]
    common_suffixes = set(candidate_suffixes)
    for variable in available_required:
        common_suffixes &= set(valid_by_variable.get(variable, []))
    ordered_suffixes = [
        suffix for suffix in _gefs_member_suffixes(hourly, "temperature_2m")
        if suffix in common_suffixes
    ]
    if not ordered_suffixes:
        ordered_suffixes = sorted(common_suffixes)
    temperature_available = bool(valid_by_variable.get("temperature_2m"))
    required_quality_issues = (
        required_array_length_mismatch
        or required_null_data_series
        or required_partial_data_series
    )
    member_check_status = "PASS" if (
        bool(times)
        and temperature_available
        and len(ordered_suffixes) == expected_members
        and not required_missing_variables
        and not required_quality_issues
    ) else "PARTIAL" if bool(times) and temperature_available and ordered_suffixes else "FAIL"
    valid = member_check_status != "FAIL"
    missing_variables = sorted(set(required_missing_variables) | set(optional_missing_variables))
    return valid, {
        "status": member_check_status,
        "expected_members": expected_members,
        "members_valid": len(ordered_suffixes),
        "member_suffixes": ordered_suffixes,
        "member_ids": [_gefs_member_id(suffix) for suffix in ordered_suffixes],
        "actual_member_counts_by_variable": variable_counts,
        "required_missing_variables": required_missing_variables,
        "optional_missing_variables": optional_missing_variables,
        "missing_variables": missing_variables,
        "unavailable_variables": sorted(set(unavailable_variables)),
        "array_length_mismatch": sorted(set(array_length_mismatch)),
        "null_data_series": sorted(set(null_data_series)),
        "partial_data_series": sorted(set(partial_data_series)),
        "required_array_length_mismatch": required_array_length_mismatch,
        "optional_array_length_mismatch": optional_array_length_mismatch,
        "required_null_data_series": required_null_data_series,
        "optional_null_data_series": optional_null_data_series,
        "required_partial_data_series": required_partial_data_series,
        "optional_partial_data_series": optional_partial_data_series,
        "valid_series_by_variable": valid_by_variable,
    }


def _ec_ensemble_member_check(hourly: dict) -> tuple[bool, dict]:
    """Member check for the 51-series ECMWF ensemble using the unified variable set."""
    return _gefs_member_check(
        hourly,
        variables=ENSEMBLE_VARIABLES,
        required_variables=EC_ENSEMBLE_REQUIRED_VARIABLES,
        optional_variables=EC_ENSEMBLE_OPTIONAL_VARIABLES,
        expected_members=EC_ENSEMBLE_TOTAL_MEMBERS,
    )


def _gefs_time_groups(times: list[str], cutoff_date: dt.date) -> dict[str, list[int]]:
    groups: dict[str, list[int]] = {}
    for index, value in enumerate(times):
        try:
            local_time = parse_local_api_time(value)
        except (TypeError, ValueError):
            continue
        if local_time.date() <= cutoff_date:
            groups.setdefault(local_time.date().isoformat(), []).append(index)
    return groups


def _gefs_member_aggregate(
    hourly: dict,
    suffix: str,
    indices: list[int],
    solar_variable: str | None = None,
) -> dict | None:
    def values(variable: str) -> list[float]:
        return _values_for_indices(hourly, _gefs_value_key(variable, suffix), indices)

    temperatures = values("temperature_2m")
    if not temperatures:
        return None
    precipitation = values("precipitation")
    rain = values("rain")
    snowfall = values("snowfall")
    clouds = values("cloud_cover")
    low_clouds = values("cloud_cover_low")
    mid_clouds = values("cloud_cover_mid")
    high_clouds = values("cloud_cover_high")
    dew_point = values("dew_point_2m")
    humidity = values("relative_humidity_2m")
    wind = values("wind_speed_10m")
    wind_direction = values("wind_direction_10m")
    gusts = values("wind_gusts_10m")
    solar = values(solar_variable) if solar_variable else []
    result = {
        "temperature_mean_c": round(mean(temperatures), 3),
        "temperature_min_c": round(min(temperatures), 3),
        "temperature_max_c": round(max(temperatures), 3),
        "dew_point_c": round(mean(dew_point), 3) if dew_point else None,
        "relative_humidity_pct": round(mean(humidity), 3) if humidity else None,
        "precipitation_mm": round(sum(precipitation), 3) if precipitation else None,
        "rain_mm": round(sum(rain), 3) if rain else None,
        "snowfall_cm": round(sum(snowfall), 3) if snowfall else None,
        "cloud_cover_pct": round(mean(clouds), 3) if clouds else None,
        "cloud_cover_low_pct": round(mean(low_clouds), 3) if low_clouds else None,
        "cloud_cover_mid_pct": round(mean(mid_clouds), 3) if mid_clouds else None,
        "cloud_cover_high_pct": round(mean(high_clouds), 3) if high_clouds else None,
        "wind_speed_kmh": round(mean(wind), 3) if wind else None,
        # Wind direction is circular: report the vector mean plus its resultant
        # length so a degenerate, direction-less sample is visible as such.
        "wind_direction_deg": circular_mean_degrees(wind_direction),
        "wind_direction_resultant_length": circular_resultant_length(wind_direction),
        "wind_gust_kmh": round(max(gusts), 3) if gusts else None,
    }
    if solar:
        result["solar_value"] = round(sum(solar), 3) if solar_variable == "sunshine_duration" else round(mean(solar), 3)
    return result


def _gefs_distribution(values: list[float], members_valid: int) -> dict:
    stats = ensemble_statistics(values)
    stats["available_members"] = len(values)
    stats["members_valid"] = members_valid
    return stats


def _gefs_probability(
    values: list[float],
    predicate,
    members_valid: int,
) -> dict:
    matching_members = sum(bool(predicate(value)) for value in values)
    return {
        "members": matching_members,
        "members_valid": members_valid,
        # An unsupported variable has no valid denominator.  Returning 0.0
        # would incorrectly turn "unavailable" into "no signal".
        "probability": round(matching_members / members_valid, 3)
        if members_valid and values
        else None,
        "available_members": len(values),
    }


def _gefs_distribution_summary(member_values: list[dict], members_valid: int) -> dict:
    def metric(name: str) -> list[float]:
        return [float(item[name]) for item in member_values if item.get(name) is not None]

    temperature_mean = metric("temperature_mean_c")
    temperature_min = metric("temperature_min_c")
    temperature_max = metric("temperature_max_c")
    dew_point = metric("dew_point_c")
    humidity = metric("relative_humidity_pct")
    precipitation = metric("precipitation_mm")
    rain = metric("rain_mm")
    snowfall = metric("snowfall_cm")
    clouds = metric("cloud_cover_pct")
    low_clouds = metric("cloud_cover_low_pct")
    mid_clouds = metric("cloud_cover_mid_pct")
    high_clouds = metric("cloud_cover_high_pct")
    gusts = metric("wind_gust_kmh")
    wind = metric("wind_speed_kmh")
    solar = metric("solar_value")
    summary = {
        "temperature_2m": _gefs_distribution(temperature_mean, members_valid),
        "temperature_2m_min": _gefs_distribution(temperature_min, members_valid),
        "temperature_2m_max": _gefs_distribution(temperature_max, members_valid),
        "dew_point_2m": _gefs_distribution(dew_point, members_valid),
        "relative_humidity_2m": _gefs_distribution(humidity, members_valid),
        "cloud_cover": _gefs_distribution(clouds, members_valid),
        "cloud_cover_low": _gefs_distribution(low_clouds, members_valid),
        "cloud_cover_mid": _gefs_distribution(mid_clouds, members_valid),
        "cloud_cover_high": _gefs_distribution(high_clouds, members_valid),
        "precipitation": _gefs_distribution(precipitation, members_valid),
        "rain": _gefs_distribution(rain, members_valid),
        "snowfall": _gefs_distribution(snowfall, members_valid),
        "wind_speed_10m": _gefs_distribution(wind, members_valid),
        "wind_gusts_10m": _gefs_distribution(gusts, members_valid),
        # Wind direction is deliberately absent: a circular quantity is not
        # summarised with arithmetic percentiles.  Its member vector means are
        # reported per window in ``wind_direction`` instead.
        "wind_direction": {
            "member_vector_means_deg": [
                item["wind_direction_deg"]
                for item in member_values
                if item.get("wind_direction_deg") is not None
            ],
            "available_members": len([
                item for item in member_values if item.get("wind_direction_deg") is not None
            ]),
            "members_valid": members_valid,
            "circular_averaging": True,
        },
        "probabilities": {
            "precipitation_gt_0_1mm": _gefs_probability(precipitation, lambda value: value > 0.1, members_valid),
            "precipitation_gt_0_5mm": _gefs_probability(precipitation, lambda value: value > 0.5, members_valid),
            "precipitation_gt_2mm": _gefs_probability(precipitation, lambda value: value > 2, members_valid),
            "precipitation_gt_5mm": _gefs_probability(precipitation, lambda value: value > 5, members_valid),
            "rain_gt_0_1mm": _gefs_probability(rain, lambda value: value > 0.1, members_valid),
            "snowfall_gt_0cm": _gefs_probability(snowfall, lambda value: value > 0, members_valid),
            "snowfall_gt_0_5cm": _gefs_probability(snowfall, lambda value: value > 0.5, members_valid),
            "snowfall_gt_1cm": _gefs_probability(snowfall, lambda value: value > 1, members_valid),
            "snowfall_gt_2cm": _gefs_probability(snowfall, lambda value: value > 2, members_valid),
            "snowfall_gt_3cm": _gefs_probability(snowfall, lambda value: value > 3, members_valid),
            "snowfall_gt_5cm": _gefs_probability(snowfall, lambda value: value > 5, members_valid),
            "gust_gt_30kmh": _gefs_probability(gusts, lambda value: value > 30, members_valid),
            "gust_gt_40kmh": _gefs_probability(gusts, lambda value: value > 40, members_valid),
            "gust_gt_50kmh": _gefs_probability(gusts, lambda value: value > 50, members_valid),
            "gust_gt_60kmh": _gefs_probability(gusts, lambda value: value > 60, members_valid),
            "cloud_cover_gt_50pct": _gefs_probability(clouds, lambda value: value > 50, members_valid),
            "cloud_cover_gt_70pct": _gefs_probability(clouds, lambda value: value > 70, members_valid),
            "cloud_cover_gt_90pct": _gefs_probability(clouds, lambda value: value > 90, members_valid),
            "cloud_cover_low_gt_30pct": _gefs_probability(low_clouds, lambda value: value > 30, members_valid),
            "cloud_cover_low_gt_50pct": _gefs_probability(low_clouds, lambda value: value > 50, members_valid),
            "cloud_cover_low_gt_70pct": _gefs_probability(low_clouds, lambda value: value > 70, members_valid),
            "cloud_cover_mid_gt_30pct": _gefs_probability(mid_clouds, lambda value: value > 30, members_valid),
            "cloud_cover_mid_gt_50pct": _gefs_probability(mid_clouds, lambda value: value > 50, members_valid),
            "cloud_cover_mid_gt_70pct": _gefs_probability(mid_clouds, lambda value: value > 70, members_valid),
            "cloud_cover_high_gt_30pct": _gefs_probability(high_clouds, lambda value: value > 30, members_valid),
            "cloud_cover_high_gt_50pct": _gefs_probability(high_clouds, lambda value: value > 50, members_valid),
            "cloud_cover_high_gt_70pct": _gefs_probability(high_clouds, lambda value: value > 70, members_valid),
            "temperature_lt_0c": _gefs_probability(temperature_min, lambda value: value < 0, members_valid),
            "temperature_lt_minus5c": _gefs_probability(temperature_min, lambda value: value < -5, members_valid),
            "daily_tmax_lt_0c": _gefs_probability(temperature_max, lambda value: value < 0, members_valid),
            "daily_tmin_lt_minus5c": _gefs_probability(temperature_min, lambda value: value < -5, members_valid),
        },
    }
    if solar:
        summary["solar"] = _gefs_distribution(solar, members_valid)
    else:
        summary["solar"] = None
    return summary


def _gefs_window_indices(times: list[str], target_date: dt.date, window: str, cutoff_date: dt.date) -> list[int]:
    indices = []
    for index, value in enumerate(times):
        try:
            local_time = parse_local_api_time(value)
        except (TypeError, ValueError):
            continue
        if local_time.date() > cutoff_date:
            continue
        if window == "MORNING" and local_time.date() == target_date and 8 <= local_time.hour < 12:
            indices.append(index)
        elif window == "AFTERNOON" and local_time.date() == target_date and 12 <= local_time.hour < 18:
            indices.append(index)
        elif window == "NIGHT":
            if local_time.date() == target_date and local_time.hour >= 18:
                indices.append(index)
            elif local_time.date() == target_date + dt.timedelta(days=1) and local_time.hour < 8 and local_time.date() <= cutoff_date:
                indices.append(index)
    return indices


def _gefs_event_values(hourly: dict, suffix: str, event_type: str, times: list[str], cutoff_date: dt.date) -> tuple[list[dt.datetime], list[float]]:
    event_times = []
    activity = []
    for index, value in enumerate(times):
        try:
            local_time = parse_local_api_time(value)
        except (TypeError, ValueError):
            continue
        if local_time.date() > cutoff_date:
            continue
        def one(variable: str) -> float | None:
            values = hourly.get(_gefs_value_key(variable, suffix))
            if not isinstance(values, list) or index >= len(values) or values[index] is None:
                return None
            return float(values[index])
        cloud = one("cloud_cover")
        low_cloud = one("cloud_cover_low")
        precipitation = one("precipitation")
        snowfall = one("snowfall")
        if event_type == "CLOUD_EVENT":
            active = (cloud is not None and cloud >= GEFS_PHASE_THRESHOLDS["cloud_cover_pct"]) or (
                low_cloud is not None and low_cloud >= GEFS_PHASE_THRESHOLDS["cloud_cover_low_pct"]
            )
            value_score = max(cloud or 0, low_cloud or 0)
        elif event_type == "PRECIP_EVENT":
            active = precipitation is not None and precipitation >= GEFS_PHASE_THRESHOLDS["precipitation_mm"]
            value_score = precipitation or 0
        elif event_type == "SNOW_EVENT":
            active = snowfall is not None and snowfall > GEFS_PHASE_THRESHOLDS["snowfall_cm"]
            value_score = snowfall or 0
        else:
            continue
        event_times.append(local_time)
        activity.append(value_score if active else 0.0)
    return event_times, activity


def _gefs_primary_interval(times: list[dt.datetime], activity: list[float]) -> dict | None:
    active_indices = [index for index, value in enumerate(activity) if value > 0]
    if not active_indices:
        return None
    intervals = []
    current = [active_indices[0]]
    for index in active_indices[1:]:
        gap = (times[index] - times[current[-1]]).total_seconds() / 3600
        if gap <= GEFS_PHASE_THRESHOLDS["maximum_event_gap_hours"]:
            current.append(index)
        else:
            intervals.append(current)
            current = [index]
    intervals.append(current)
    candidates = []
    for interval in intervals:
        duration = (times[interval[-1]] - times[interval[0]]).total_seconds() / 3600 + 1
        if duration < GEFS_PHASE_THRESHOLDS["minimum_event_duration_hours"]:
            continue
        peak_index = max(interval, key=lambda index: activity[index])
        candidates.append({
            "event_start": times[interval[0]],
            "event_peak": times[peak_index],
            "event_end": times[interval[-1]],
            "peak_value": round(activity[peak_index], 3),
            "duration_hours": round(duration, 3),
        })
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item["duration_hours"], item["peak_value"]))


def _gefs_cold_interval(member_days: dict[str, dict]) -> dict | None:
    ordered = sorted(member_days.items())
    active = []
    previous = None
    for day, values in ordered:
        mean_value = values.get("temperature_mean_c")
        min_value = values.get("temperature_min_c")
        drop = (
            float(mean_value) - float(previous.get("temperature_mean_c"))
            if previous and mean_value is not None and previous.get("temperature_mean_c") is not None
            else 0
        )
        is_active = drop <= -GEFS_PHASE_THRESHOLDS["cold_daily_mean_drop_c"] or (
            min_value is not None and float(min_value) <= GEFS_PHASE_THRESHOLDS["cold_daily_tmin_c"]
        )
        if is_active:
            active.append(day)
        previous = values
    if not active:
        return None
    start = dt.datetime.combine(dt.date.fromisoformat(active[0]), dt.time(0), tzinfo=LOCAL_TZ)
    end = dt.datetime.combine(dt.date.fromisoformat(active[-1]), dt.time(23, 0), tzinfo=LOCAL_TZ)
    peak_day = min(active, key=lambda value: member_days[value].get("temperature_mean_c", 999))
    peak = dt.datetime.combine(dt.date.fromisoformat(peak_day), dt.time(12), tzinfo=LOCAL_TZ)
    return {"event_start": start, "event_peak": peak, "event_end": end, "peak_value": round(float(member_days[peak_day].get("temperature_mean_c", 0)), 3), "duration_hours": round((end - start).total_seconds() / 3600 + 1, 3)}


def _gefs_iso(value: dt.datetime | None) -> str | None:
    return value.astimezone(LOCAL_TZ).isoformat(timespec="minutes") if value else None


def _gefs_time_stats(values: list[dt.datetime]) -> dict:
    if not values:
        return {"p25": None, "median": None, "p75": None, "earliest": None, "latest": None}
    timestamps = [value.timestamp() for value in values]
    return {
        "p25": _gefs_iso(dt.datetime.fromtimestamp(percentile(timestamps, 0.25), tz=LOCAL_TZ)),
        "median": _gefs_iso(dt.datetime.fromtimestamp(percentile(timestamps, 0.5), tz=LOCAL_TZ)),
        "p75": _gefs_iso(dt.datetime.fromtimestamp(percentile(timestamps, 0.75), tz=LOCAL_TZ)),
        "earliest": _gefs_iso(min(values)),
        "latest": _gefs_iso(max(values)),
    }


def _gefs_phase_confidence(support: float, spread_hours: float, multimodal: bool) -> str:
    if multimodal:
        return "LOW"
    if support >= GEFS_PHASE_THRESHOLDS["high_phase_support"] and spread_hours <= GEFS_PHASE_THRESHOLDS["high_phase_spread_hours"]:
        return "HIGH"
    if support >= GEFS_PHASE_THRESHOLDS["medium_phase_support"] and spread_hours <= GEFS_PHASE_THRESHOLDS["medium_phase_spread_hours"]:
        return "MEDIUM"
    return "LOW"


def _gefs_event_phase(
    event_type: str,
    events: list[dict],
    members_valid: int,
) -> dict:
    if not events or not members_valid:
        return {
            "status": "NO_SIGNAL",
            "rule_version": GEFS_PHASE_RULE_VERSION,
            "event_type": event_type,
            "members_with_event": 0,
            "members_valid": members_valid,
            "member_support": 0.0 if members_valid else None,
            "phase_confidence": "LOW",
            "phase_spread_hours": None,
            "multimodal": False,
            "event_start": _gefs_time_stats([]),
            "event_peak": _gefs_time_stats([]),
            "event_end": _gefs_time_stats([]),
            "event_day_distribution": {"none": {"members": members_valid, "percentage": 1.0} if members_valid else {}},
        }
    starts = [item["event_start"] for item in events]
    peaks = [item["event_peak"] for item in events]
    ends = [item["event_end"] for item in events]
    support = len(events) / members_valid
    peak_bins: dict[str, int] = {}
    for value in peaks:
        key = value.date().isoformat()
        peak_bins[key] = peak_bins.get(key, 0) + 1
    distribution = {
        key: {"members": count, "percentage": round(count / members_valid, 3)}
        for key, count in sorted(peak_bins.items())
    }
    none_count = max(0, members_valid - len(events))
    if none_count:
        distribution["none"] = {"members": none_count, "percentage": round(none_count / members_valid, 3)}
    sorted_peaks = sorted(peaks)
    multimodal = False
    if len(peak_bins) >= 2:
        strong_bins = [key for key, count in peak_bins.items() if count / members_valid >= 0.2]
        if len(strong_bins) >= 2:
            multimodal = (dt.date.fromisoformat(max(strong_bins)) - dt.date.fromisoformat(min(strong_bins))).days >= 1
    start_stats = _gefs_time_stats(starts)
    peak_stats = _gefs_time_stats(peaks)
    end_stats = _gefs_time_stats(ends)
    spread_candidates = []
    for stats in (start_stats, peak_stats, end_stats):
        if stats["p25"] and stats["p75"]:
            spread_candidates.append(
                (dt.datetime.fromisoformat(stats["p75"]) - dt.datetime.fromisoformat(stats["p25"])).total_seconds() / 3600
            )
    spread = round(max(spread_candidates), 3) if spread_candidates else None
    spread_for_confidence = spread if spread is not None else 999
    return {
        "status": "SIGNAL",
        "rule_version": GEFS_PHASE_RULE_VERSION,
        "event_type": event_type,
        "members_with_event": len(events),
        "members_valid": members_valid,
        "member_support": round(support, 3),
        "phase_confidence": _gefs_phase_confidence(support, spread_for_confidence, multimodal),
        "phase_spread_hours": spread,
        "multimodal": multimodal,
        "event_start": start_stats,
        "event_peak": peak_stats,
        "event_end": end_stats,
        "event_day_distribution": distribution,
    }


def _gefs_event_phases(hourly: dict, member_suffixes: list[str], member_daily: dict[str, dict[str, dict]], cutoff_date: dt.date) -> dict:
    times = hourly.get("time") or []
    result = {}
    for event_type in ("CLOUD_EVENT", "PRECIP_EVENT", "SNOW_EVENT"):
        events = []
        for suffix in member_suffixes:
            event_times, activity = _gefs_event_values(hourly, suffix, event_type, times, cutoff_date)
            interval = _gefs_primary_interval(event_times, activity)
            if interval:
                interval["member_id"] = _gefs_member_id(suffix)
                events.append(interval)
        result[event_type] = _gefs_event_phase(event_type, events, len(member_suffixes))
    cold_events = []
    for suffix in member_suffixes:
        daily = {
            date_key: values.get(suffix)
            for date_key, values in member_daily.items()
            if values.get(suffix)
        }
        interval = _gefs_cold_interval(daily)
        if interval:
            interval["member_id"] = _gefs_member_id(suffix)
            cold_events.append(interval)
    result["COLD_EVENT"] = _gefs_event_phase("COLD_EVENT", cold_events, len(member_suffixes))
    return result


def _build_gefs_segment(record: dict, segment_key: str, model_id: str, model: str, resolution: str, cutoff_date: dt.date) -> dict:
    hourly = record.get("hourly") or {}
    times = hourly.get("time") if isinstance(hourly.get("time"), list) else []
    member_valid, member_check = _gefs_member_check(hourly)
    suffixes = member_check.get("member_suffixes", [])
    groups = _gefs_time_groups(times, cutoff_date)
    solar_variable = record.get("solar_variable") if record.get("solar_variable") in GEFS_OPTIONAL_SOLAR_VARIABLES else None
    member_daily: dict[str, dict[str, dict]] = {}
    daily_public = []
    for day, indices in sorted(groups.items()):
        per_member = {}
        for suffix in suffixes:
            aggregate = _gefs_member_aggregate(hourly, suffix, indices, solar_variable)
            if aggregate:
                per_member[suffix] = aggregate
        member_daily[day] = per_member
        daily_public.append({
            "date": day,
            "members_valid": len(suffixes),
            "statistics": _gefs_distribution_summary(list(per_member.values()), len(suffixes)),
        })
    window_public: dict[str, dict] = {}
    for day in sorted(groups):
        target = dt.date.fromisoformat(day)
        window_public[day] = {}
        for window_name in GEFS_WINDOW_DEFINITIONS:
            indices = _gefs_window_indices(times, target, window_name, cutoff_date)
            per_member = {}
            for suffix in suffixes:
                aggregate = _gefs_member_aggregate(hourly, suffix, indices, solar_variable)
                if aggregate:
                    per_member[suffix] = aggregate
            window_public[day][window_name] = {
                "date": day,
                "window": window_name,
                "members_valid": len(suffixes),
                "status": "OK" if per_member else "UNAVAILABLE",
                "statistics": _gefs_distribution_summary(list(per_member.values()), len(suffixes)),
            }
    phase = _gefs_event_phases(hourly, suffixes, member_daily, cutoff_date)
    forecast_start = times[0] if times else None
    forecast_end = times[-1] if times else None
    required_missing_variables = sorted(set(member_check.get("required_missing_variables", [])))
    optional_missing_variables = sorted(
        set(member_check.get("optional_missing_variables", []))
        | set(record.get("gefs_missing_variables", []))
    )
    missing_variables = sorted(set(required_missing_variables) | set(optional_missing_variables))
    segment_status = "FAILED" if not member_valid else "OK" if member_check.get("status") == "PASS" and not required_missing_variables else "PARTIAL"
    raw_availability = variable_availability(hourly, GEFS_CORE_VARIABLES)
    variable_status = variable_status_classification(
        raw_availability,
        required_variables=GEFS_REQUIRED_VARIABLES,
        optional_variables=GEFS_OPTIONAL_VARIABLES,
    )
    for variable in optional_missing_variables:
        if variable not in variable_status:
            variable_status[variable] = VARIABLE_STATUS_OPTIONAL_UNAVAILABLE
        elif variable_status[variable] == VARIABLE_STATUS_OK:
            variable_status[variable] = VARIABLE_STATUS_PARTIAL
    # Recompute after the capability probe has been folded in, so the published
    # unavailable lists and variable_status never contradict each other.
    unavailable = unavailable_variables_for(
        variable_status,
        required_variables=GEFS_REQUIRED_VARIABLES,
        optional_variables=GEFS_OPTIONAL_VARIABLES,
    )
    cloud_layer_status = {
        variable: variable_status.get(variable, VARIABLE_STATUS_MISSING)
        for variable in CLOUD_LAYER_VARIABLES
    }
    return {
        "status": segment_status,
        "segment": segment_key,
        "source": "Open-Meteo",
        "model": model,
        "model_id": model_id,
        "resolution": resolution,
        "run_time": (record.get("response") or {}).get("model_run_initialization"),
        "generated_at": (record.get("response") or {}).get("retrieval_time"),
        "forecast_start": forecast_start,
        "forecast_end": forecast_end,
        "forecast_start_date": parse_local_api_time(forecast_start).date().isoformat() if forecast_start else None,
        "forecast_end_date": parse_local_api_time(forecast_end).date().isoformat() if forecast_end else None,
        "members_total": GEFS_ENSEMBLE_MEMBERS,
        "members_valid": len(suffixes),
        "missing_variables": missing_variables,
        "required_missing_variables": required_missing_variables,
        "optional_missing_variables": optional_missing_variables,
        "requested_variables": list(GEFS_CORE_VARIABLES),
        "required_variables": list(GEFS_REQUIRED_VARIABLES),
        "optional_variables": list(GEFS_OPTIONAL_VARIABLES),
        "variable_status": variable_status,
        # Layered cloud is requested but not derived: when Open-Meteo GEFS
        # returns null arrays for western Sichuan the layers stay explicitly
        # unavailable instead of being inferred from the total cloud cover.
        "cloud_layer_status": cloud_layer_status,
        **unavailable,
        "daily": daily_public,
        "windows": window_public,
        "event_phases": phase,
        "event_day_distribution": {
            event_type.lower(): value.get("event_day_distribution", {})
            for event_type, value in phase.items()
        },
        "stale": bool(record.get("stale")),
        "cached_generated_at": record.get("cached_generated_at"),
        "qa": {
            "grid_scale_class": "coarse_ensemble" if segment_key == "long_range" else "medium_ensemble",
            "expected_ensemble_members": GEFS_ENSEMBLE_MEMBERS,
            "actual_ensemble_members": len(suffixes),
            "member_series_check": member_check,
            "timezone": (record.get("response") or {}).get("timezone"),
            "utc_offset_seconds": (record.get("response") or {}).get("utc_offset_seconds"),
            "forecast_cutoff_date": cutoff_date.isoformat(),
            "forecast_cutoff_applied": True,
            "last_available_timestamp": forecast_end,
            "probability_denominator": "members_valid",
            "final_status": "PASS" if segment_status == "OK" else segment_status,
        },
    }


def _gefs_cache_path(model_id: str, point_id: str, cache_dir: Path | None = None) -> Path:
    return (cache_dir or GEFS_CACHE_DIR) / model_id / f"{point_id}.json"


def _gefs_cache_identity(point: dict, model_id: str, variables: list[str]) -> dict:
    return {
        "model_id": model_id,
        "requested_coordinate": {"latitude": point.get("latitude"), "longitude": point.get("longitude")},
        "timezone": TIMEZONE_NAME,
        "cell_selection": "nearest",
        "elevation": "nan",
        "variables": list(variables),
    }


def _load_gefs_cache(point: dict, model_id: str, variables: list[str], cache_dir: Path | None = None) -> dict | None:
    path = _gefs_cache_path(model_id, point["id"], cache_dir)
    if not path.is_file():
        return None
    try:
        with path.open(encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if value.get("cache_identity") != _gefs_cache_identity(point, model_id, variables):
        return None
    record = value.get("record")
    return copy.deepcopy(record) if isinstance(record, dict) else None


def _write_gefs_cache(point: dict, model_id: str, variables: list[str], record: dict, cache_dir: Path | None = None) -> None:
    path = _gefs_cache_path(model_id, point["id"], cache_dir)
    write_json(path, {
        "schema_version": SCHEMA_VERSION,
        "cache_kind": "gefs_forecast_response",
        "cache_identity": _gefs_cache_identity(point, model_id, variables),
        "cached_generated_at": record.get("response", {}).get("retrieval_time"),
        "record": record,
    })


def _record_has_usable_series(record: dict, variable: str) -> bool:
    """True when the primary response already carries usable values for a variable."""
    hourly = record.get("hourly") or {}
    values = hourly.get(variable)
    times = hourly.get("time")
    if not isinstance(values, list) or not isinstance(times, list) or len(values) != len(times):
        return False
    return any(value is not None for value in values)


def _fetch_gefs_optional_solar(
    client: ApiClient,
    record: dict,
    point: dict,
    params: dict[str, object],
    label: str,
) -> None:
    missing = []
    warnings = []
    # The current Ensemble API does not document solar variables.  Make one
    # explicit probe, then report both alternatives as unavailable rather than
    # spending another retry cycle on a known unsupported field.
    for variable in GEFS_OPTIONAL_SOLAR_VARIABLES[:1]:
        try:
            payload, url = client.get_json(
                OPEN_METEO_ENDPOINTS["ensemble"],
                {**params, "hourly": variable},
                f"{label}:{variable}",
            )
        except OpenMeteoError as error:
            missing.extend(_solar_variables_still_missing(record))
            warnings.append(f"{variable}:{error.reason}")
            break
        hourly = payload.get("hourly") if isinstance(payload.get("hourly"), dict) else {}
        values = hourly.get(variable)
        primary_response = record.get("response") or {}
        if (
            payload.get("timezone") != TIMEZONE_NAME
            or payload.get("utc_offset_seconds") != 28800
            or (payload.get("latitude"), payload.get("longitude")) != (
                primary_response.get("grid_coordinate", {}).get("latitude"),
                primary_response.get("grid_coordinate", {}).get("longitude"),
            )
            or not isinstance(values, list)
            or len(values) != len(record.get("hourly", {}).get("time", []))
            or any(value is None for value in values)
        ):
            missing.extend(_solar_variables_still_missing(record))
            warnings.append(f"{variable}:OPTIONAL_VARIABLE_QA_FAIL")
            break
        record.setdefault("hourly", {})[variable] = values
        record["solar_variable"] = variable
        record.setdefault("response", {}).setdefault("optional_variable_urls", {})[variable] = url
        break
    record["gefs_missing_variables"] = sorted(set(record.get("gefs_missing_variables", [])) | set(missing))
    record.setdefault("qa", {}).setdefault("warnings", []).extend(warnings)


def _solar_variables_still_missing(record: dict) -> list[str]:
    """Solar alternatives the primary response did not already deliver.

    The probe only tests one alternative, so a failure must not mark a solar
    variable that the unified request already returned with usable values.
    """
    return [
        variable
        for variable in GEFS_OPTIONAL_SOLAR_VARIABLES
        if not _record_has_usable_series(record, variable)
    ]


def _gefs_solar_available(record: dict) -> bool:
    """True when the main GEFS request already returned a usable solar series."""
    return _record_has_usable_series(record, "sunshine_duration")


def classify_record_variable_status(
    record: dict,
    *,
    required_variables,
    optional_variables,
) -> dict:
    """Publish the module-level required/optional view of one fetched record."""
    availability = record.get("raw_variable_availability")
    if not isinstance(availability, dict) or not availability:
        availability = variable_availability(record.get("hourly") or {}, record.get("requested_variables") or [])
        record["raw_variable_availability"] = availability
    status = variable_status_classification(
        availability,
        required_variables=required_variables,
        optional_variables=optional_variables,
    )
    record["variable_status"] = status
    record.update(
        unavailable_variables_for(
            status,
            required_variables=required_variables,
            optional_variables=optional_variables,
        )
    )
    return status


def _fetch_gefs_segment(
    config: dict,
    point: dict,
    client: ApiClient,
    segment_key: str,
    model_id: str,
    model: str,
    resolution: str,
    forecast_days: int,
    generated_at: str,
    cutoff_date: dt.date,
    *,
    cache_dir: Path | None = None,
    solar_capabilities: dict[str, bool] | None = None,
) -> dict:
    params = base_weather_params(point, models=model_id, forecast_days=forecast_days)
    record = fetch_point(
        client,
        point=point,
        source="Open-Meteo",
        endpoint=OPEN_METEO_ENDPOINTS["ensemble"],
        model=model,
        params=params,
        variables=GEFS_CORE_VARIABLES,
        required_variables=["temperature_2m"],
        optional_variables=GEFS_OPTIONAL_VARIABLES,
        grid_limit_km=GEFS_GRID_QA_LIMITS_KM[segment_key],
        log_label=f"{point['id']}:GEFS_{segment_key.upper()}",
        accepted_model_values=(model_id, model),
        accepted_model_ids=(model_id,),
        max_forecast_date=cutoff_date,
    )
    used_cache = False
    if record.get("status") != "PASS":
        cached = _load_gefs_cache(point, model_id, GEFS_CORE_VARIABLES, cache_dir)
        if cached:
            record = cached
            record["stale"] = True
            record["cached_generated_at"] = (cached.get("response") or {}).get("retrieval_time")
            used_cache = True
            log(f"[{point['id']}:GEFS_{segment_key.upper()}] STALE CACHE FALLBACK")
    if record.get("status") == "PASS" and not used_cache:
        capability = solar_capabilities.get(model_id) if solar_capabilities is not None else None
        if capability is not False:
            _fetch_gefs_optional_solar(client, record, point, params, f"{point['id']}:GEFS_{segment_key.upper()}")
            if solar_capabilities is not None:
                solar_capabilities[model_id] = bool(record.get("solar_variable"))
        else:
            record["gefs_missing_variables"] = sorted(
                set(record.get("gefs_missing_variables", []))
                | set(_solar_variables_still_missing(record))
            )
        # The unified GEFS request already asks for sunshine_duration, so a
        # usable series in the primary response is authoritative even when the
        # standalone capability probe was inconclusive.
        if not record.get("solar_variable") and _gefs_solar_available(record):
            record["solar_variable"] = "sunshine_duration"
        classify_record_variable_status(
            record,
            required_variables=GEFS_REQUIRED_VARIABLES,
            optional_variables=GEFS_OPTIONAL_VARIABLES,
        )
        _write_gefs_cache(point, model_id, GEFS_CORE_VARIABLES, record, cache_dir)
    record.setdefault("gefs_missing_variables", [])
    record.setdefault("gefs_segment", segment_key)
    record.setdefault("gefs_model_id", model_id)
    if record.get("status") == "PASS" and (used_cache or "variable_status" not in record):
        classify_record_variable_status(
            record,
            required_variables=GEFS_REQUIRED_VARIABLES,
            optional_variables=GEFS_OPTIONAL_VARIABLES,
        )
    return record


def run_gefs(
    config: dict,
    client: ApiClient,
    generated_at: str,
    data_date: str,
    *,
    cutoff_date: dt.date = GEFS_TRAVEL_CUTOFF_DATE,
    cache_dir: Path | None = None,
) -> dict:
    """Fetch the independent GEFS chain and publish member-distribution evidence."""
    points = active_points(config)
    raw_points: dict[str, dict] = {}
    public_points: dict[str, dict] = {}
    near_success = 0
    long_success = 0
    successful_points = 0
    partial_points = 0
    failed_points = 0
    usable_points = 0
    missing_variables: set[str] = set()
    required_missing_variables: set[str] = set()
    optional_missing_variables: set[str] = set()
    required_unavailable_variables: set[str] = set()
    optional_unavailable_variables: set[str] = set()
    qa_warnings: list[str] = []
    point_status_summary: dict[str, dict] = {}
    solar_capabilities: dict[str, bool] = {}
    coverage_starts = []
    coverage_ends = []
    stale = False
    for point_id, point in points.items():
        segment_records = {}
        segment_public = {}
        for segment_key, model_id, model, resolution, forecast_days in (
            ("near_range", GEFS_NEAR_MODEL_ID, GEFS_NEAR_MODEL, GEFS_NEAR_RESOLUTION, GEFS_NEAR_FORECAST_DAYS),
            ("long_range", GEFS_LONG_MODEL_ID, GEFS_LONG_MODEL, GEFS_LONG_RESOLUTION, GEFS_LONG_FORECAST_DAYS),
        ):
            record = _fetch_gefs_segment(
                config,
                point,
                client,
                segment_key,
                model_id,
                model,
                resolution,
                forecast_days,
                generated_at,
                cutoff_date,
                cache_dir=cache_dir,
                solar_capabilities=solar_capabilities,
            )
            segment_records[segment_key] = record
            if record.get("stale"):
                stale = True
            segment = _build_gefs_segment(record, segment_key, model_id, model, resolution, cutoff_date)
            segment_public[segment_key] = segment
            missing_variables.update(segment.get("missing_variables") or [])
            required_missing_variables.update(segment.get("required_missing_variables") or [])
            optional_missing_variables.update(segment.get("optional_missing_variables") or [])
            required_unavailable_variables.update(segment.get("required_unavailable_variables") or [])
            optional_unavailable_variables.update(segment.get("optional_unavailable_variables") or [])
            qa_warnings.extend(
                f"{point_id}:{segment_key}:{variable}:OPTIONAL_UNAVAILABLE"
                for variable in (segment.get("optional_missing_variables") or [])
            )
            qa_warnings.extend(
                f"{point_id}:{segment_key}:{variable}:REQUIRED_UNAVAILABLE"
                for variable in (segment.get("required_missing_variables") or [])
            )
            qa_warnings.extend(
                f"{point_id}:{segment_key}:{warning}"
                for warning in (record.get("qa") or {}).get("warnings", [])
            )
            if segment.get("status") in {"OK", "PARTIAL"}:
                if segment_key == "near_range":
                    near_success += 1
                else:
                    long_success += 1
                if segment.get("forecast_start_date"):
                    coverage_starts.append(segment["forecast_start_date"])
                if segment.get("forecast_end_date"):
                    coverage_ends.append(segment["forecast_end_date"])
        raw_points[point_id] = {"near_range": segment_records["near_range"], "long_range": segment_records["long_range"]}
        segment_statuses = [value.get("status") for value in segment_public.values()]
        point_status = "OK" if all(value == "OK" for value in segment_statuses) else "PARTIAL" if any(value in {"OK", "PARTIAL"} for value in segment_statuses) else "FAILED"
        if point_status == "OK":
            successful_points += 1
        elif point_status == "PARTIAL":
            partial_points += 1
        else:
            failed_points += 1
        point_usable = point_status in {"OK", "PARTIAL"} and any(
            value.get("members_valid", 0) > 0 and value.get("status") in {"OK", "PARTIAL"}
            for value in segment_public.values()
        )
        if point_usable:
            usable_points += 1
        point_status_summary[point_id] = {
            "status": point_status,
            "usable": point_usable,
            "required_missing_variables": sorted({
                variable
                for value in segment_public.values()
                for variable in (value.get("required_missing_variables") or [])
            }),
            "optional_missing_variables": sorted({
                variable
                for value in segment_public.values()
                for variable in (value.get("optional_missing_variables") or [])
            }),
            "variable_status": aggregate_variable_status([
                value.get("variable_status") or {} for value in segment_public.values()
            ]),
            "cloud_layer_status": {
                variable: next(
                    (
                        (value.get("cloud_layer_status") or {}).get(variable)
                        for value in segment_public.values()
                        if (value.get("cloud_layer_status") or {}).get(variable)
                    ),
                    None,
                )
                for variable in CLOUD_LAYER_VARIABLES
            },
            "stale": any(value.get("stale") for value in segment_public.values()),
        }
        public_points[point_id] = {
            "point_id": point_id,
            "point": {
                "name": point.get("name"),
                "region": point.get("region"),
                "status": point.get("status"),
                "latitude": point.get("latitude"),
                "longitude": point.get("longitude"),
            },
            "status": point_status,
            "usable_for_main_chain": True,
            "near_range": segment_public["near_range"],
            "long_range": segment_public["long_range"],
            "qa": {
                "requested_coordinate": {"latitude": point.get("latitude"), "longitude": point.get("longitude")},
                "segments": {key: value.get("qa") for key, value in segment_public.items()},
                "final_status": point_status,
                "stale": any(value.get("stale") for value in segment_public.values()),
            },
        }
    for point_id, point in config.get("points", {}).items():
        if point_id not in public_points:
            log(f"[{point_id}] GEFS SKIPPED: {point.get('status', 'NOT_VERIFIED')}")
    if not points or successful_points + partial_points == 0:
        module_status = "FAILED"
    elif failed_points or partial_points or near_success < len(points) or long_success < len(points):
        module_status = "PARTIAL"
    else:
        module_status = "OK"
    return module_header(
        "gefs",
        generated_at,
        data_date,
        module_status,
        source="Open-Meteo",
        endpoint=OPEN_METEO_ENDPOINTS["ensemble"],
        model="NOAA GFS Ensemble (independent GEFS chain)",
        near_range_model=GEFS_NEAR_MODEL,
        near_range_model_id=GEFS_NEAR_MODEL_ID,
        near_range_resolution=GEFS_NEAR_RESOLUTION,
        near_range_forecast_days=GEFS_NEAR_FORECAST_DAYS,
        long_range_model=GEFS_LONG_MODEL,
        long_range_model_id=GEFS_LONG_MODEL_ID,
        long_range_resolution=GEFS_LONG_RESOLUTION,
        long_range_forecast_days=GEFS_LONG_FORECAST_DAYS,
        members_total=GEFS_ENSEMBLE_MEMBERS,
        members_valid=min(
            [
                segment.get("members_valid")
                for point in public_points.values()
                for segment in (point.get("near_range"), point.get("long_range"))
                if isinstance(segment, dict) and segment.get("members_valid")
            ]
            or [None]
        ),
        coverage_start=min(coverage_starts) if coverage_starts else None,
        coverage_end=min(max(coverage_ends), cutoff_date.isoformat()) if coverage_ends else None,
        forecast_cutoff_date=cutoff_date.isoformat(),
        missing_variables=sorted(missing_variables),
        requested_variables=list(GEFS_CORE_VARIABLES),
        required_variables=list(GEFS_REQUIRED_VARIABLES),
        optional_variables=list(GEFS_OPTIONAL_VARIABLES),
        variable_status=aggregate_variable_status([
            (point.get("near_range") or {}).get("variable_status") or {}
            for point in public_points.values()
        ] + [
            (point.get("long_range") or {}).get("variable_status") or {}
            for point in public_points.values()
        ]),
        cloud_layer_status={
            variable: next(
                (
                    ((point.get("near_range") or {}).get("cloud_layer_status") or {}).get(variable)
                    or ((point.get("long_range") or {}).get("cloud_layer_status") or {}).get(variable)
                    for point in public_points.values()
                    if ((point.get("near_range") or {}).get("cloud_layer_status") or {}).get(variable)
                    or ((point.get("long_range") or {}).get("cloud_layer_status") or {}).get(variable)
                ),
                None,
            )
            for variable in CLOUD_LAYER_VARIABLES
        },
        qa_warnings=sorted(set(qa_warnings)),
        stale=stale,
        cache={
            "enabled": True,
            "directory": str((cache_dir or GEFS_CACHE_DIR).relative_to(ROOT)) if (cache_dir or GEFS_CACHE_DIR).is_relative_to(ROOT) else str(cache_dir or GEFS_CACHE_DIR),
            "key_fields": ["model_id", "requested_coordinate", "timezone", "cell_selection", "elevation", "variables"],
            "stale_fallback_allowed": True,
        },
        points=public_points,
        raw_points=raw_points,
        excluded_points=excluded_points(config),
        successful_points=successful_points,
        partial_points=partial_points,
        failed_points=failed_points,
        usable_points=usable_points,
        required_missing_variables=sorted(required_missing_variables),
        optional_missing_variables=sorted(optional_missing_variables),
        # Same capability probe as the per-segment variable_status, published at
        # module level so optional_unavailable_variables never disagrees with
        # the OPTIONAL_UNAVAILABLE entries the module already reports.
        required_unavailable_variables=sorted(required_unavailable_variables),
        optional_unavailable_variables=sorted(optional_unavailable_variables),
        point_status_summary=point_status_summary,
        qa={
            "near_range_successful_points": near_success,
            "long_range_successful_points": long_success,
            "expected_members": GEFS_ENSEMBLE_MEMBERS,
            "probability_denominator": "members_valid",
            "forecast_cutoff_date": cutoff_date.isoformat(),
            "formal_summary_cutoff_enforced": True,
            "independent_from_ecmwf_ensemble": True,
            "final_status": module_status,
        },
        interpretation_boundary="GEFS distributions and event phases are probabilistic support for GFS deterministic output; they are not point-level precise forecasts or phenology conclusions.",
    )


LONG_RANGE_SIGNAL_ORDER = ("NONE", "WEAK", "MODERATE", "STRONG")
LONG_RANGE_UNCERTAINTY_ORDER = ("LOW", "MODERATE", "HIGH", "VERY_HIGH")


def long_range_window_definitions() -> list[tuple[int, int]]:
    windows = []
    start = LONG_RANGE_LEAD_START
    while start <= LONG_RANGE_LEAD_END:
        end = min(start + 2, LONG_RANGE_LEAD_END)
        windows.append((start, end))
        start = end + 1
    return windows


def validate_long_range_members(record: dict) -> tuple[bool, dict]:
    hourly = record.get("hourly") or {}
    times = hourly.get("time") if isinstance(hourly.get("time"), list) else []
    series_by_variable = {}
    variable_availability = {}
    missing_or_wrong_count = []
    array_length_mismatch = []
    null_data_series = []
    member_counts = {}
    for variable in LONG_RANGE_VARIABLES:
        keys = ensemble_series_keys(hourly, variable)
        series_by_variable[variable] = keys
        member_counts[variable] = len(keys)
        if len(keys) != LONG_RANGE_ENSEMBLE_MEMBERS:
            missing_or_wrong_count.append(
                f"{variable}:expected_{LONG_RANGE_ENSEMBLE_MEMBERS}_got_{len(keys)}"
            )
        first_indices = []
        last_indices = []
        for key in keys:
            values = hourly.get(key)
            if not isinstance(values, list) or len(values) != len(times):
                array_length_mismatch.append(key)
                continue
            non_null_indices = [index for index, value in enumerate(values) if value is not None]
            if not non_null_indices:
                null_data_series.append(key)
                continue
            first_index = non_null_indices[0]
            last_index = non_null_indices[-1]
            first_indices.append(first_index)
            last_indices.append(last_index)
            if any(value is None for value in values[first_index : last_index + 1]):
                null_data_series.append(key)
        if first_indices and last_indices:
            first_common = max(first_indices)
            last_common = min(last_indices)
            variable_availability[variable] = {
                "first_timestamp": times[first_common],
                "last_timestamp": times[last_common],
                "first_complete_index": first_common,
                "last_complete_index": last_common,
                "all_members_complete_through_index": last_common,
                "edge_truncated": first_common > 0 or last_common < len(times) - 1,
            }
        else:
            variable_availability[variable] = {
                "first_timestamp": None,
                "last_timestamp": None,
                "first_complete_index": None,
                "last_complete_index": None,
                "all_members_complete_through_index": None,
                "edge_truncated": False,
            }
    valid = bool(times) and not missing_or_wrong_count and not array_length_mismatch and not null_data_series
    return valid, {
        "status": "PASS" if valid else "FAIL",
        "expected_members": LONG_RANGE_ENSEMBLE_MEMBERS,
        "actual_member_counts_by_variable": member_counts,
        "series_by_variable": series_by_variable,
        "variable_availability": variable_availability,
        "edge_truncated_variables": [
            variable for variable, item in variable_availability.items() if item["edge_truncated"]
        ],
        "missing_or_wrong_count": missing_or_wrong_count,
        "array_length_mismatch": array_length_mismatch,
        "null_data_series": null_data_series,
    }


def trim_long_range_member_edges(record: dict) -> dict:
    hourly = record.get("hourly") or {}
    times = hourly.get("time") or []
    audit = {}
    for variable in LONG_RANGE_VARIABLES:
        keys = ensemble_series_keys(hourly, variable)
        bounds = []
        for key in keys:
            values = hourly.get(key) or []
            non_null = [index for index, value in enumerate(values) if value is not None]
            if non_null:
                bounds.append((non_null[0], non_null[-1]))
        first_common = max((item[0] for item in bounds), default=None)
        last_common = min((item[1] for item in bounds), default=None)
        audit[variable] = {
            "series_count": len(keys),
            "original_timestep_count": len(times),
            "all_members_first_complete_timestamp": times[first_common] if first_common is not None and first_common < len(times) else None,
            "all_members_last_complete_timestamp": times[last_common] if last_common is not None and last_common < len(times) else None,
            "leading_missing_rows": first_common or 0 if first_common is not None else None,
            "trailing_missing_rows": len(times) - 1 - last_common if last_common is not None else None,
            "horizon_status": "TRUNCATED_EDGE_MISSING" if first_common not in {None, 0} or last_common not in {None, len(times) - 1} else "COMPLETE",
        }
    record.setdefault("response", {}).update({"member_edge_audit": audit})
    record.setdefault("qa", {})["member_edge_audit"] = audit
    record["daily"] = daily_metrics(hourly)
    return record


def long_range_daily_member_values(hourly: dict) -> tuple[dt.date, dict[int, dict[str, dict]]]:
    times = hourly.get("time") or []
    if not times:
        raise ValueError("long-range hourly time is empty")
    origin_date = parse_local_api_time(times[0]).date()
    groups: dict[str, list[int]] = {}
    for index, value in enumerate(times):
        day = parse_local_api_time(value).date().isoformat()
        groups.setdefault(day, []).append(index)
    temperature_keys = ensemble_series_keys(hourly, "temperature_2m")
    daily_by_lead: dict[int, dict[str, dict]] = {}
    for day, indices in sorted(groups.items()):
        day_date = dt.date.fromisoformat(day)
        lead_day = (day_date - origin_date).days
        daily_by_lead[lead_day] = {}
        for key in temperature_keys:
            temperatures = _complete_values_for_indices(hourly, key, indices)
            suffix = key.removeprefix("temperature_2m")
            precipitation = _complete_values_for_indices(hourly, f"precipitation{suffix}", indices)
            snowfall = _complete_values_for_indices(hourly, f"snowfall{suffix}", indices)
            gusts = _complete_values_for_indices(hourly, f"wind_gusts_10m{suffix}", indices)
            if not temperatures:
                continue
            daily_by_lead[lead_day][key] = {
                "date": day,
                "temperature_mean_c": round(mean(temperatures), 3),
                "temperature_min_c": round(min(temperatures), 3),
                "precipitation_mm": round(sum(precipitation), 3) if precipitation else None,
                "snowfall_cm": round(sum(snowfall), 3) if snowfall else None,
                "wind_gust_max_kmh": round(max(gusts), 3) if gusts else None,
            }
    return origin_date, daily_by_lead


def long_range_horizon_check(daily_by_lead: dict[int, dict[str, dict]]) -> dict:
    """Judge the usable long-range horizon without failing on the model edge.

    The published 3-day blocks run to D34_D35, but the trailing block needs lead
    day 35, which only the freshest `ncep_gefs05` long run carries.  The daily
    run happens before that run is disseminated, so the required coverage is the
    last block the product reliably populates (D31_D33).  A missing trailing
    lead day is recorded as an edge shortfall, never as a module failure.
    """
    leads = sorted(daily_by_lead)
    expected = list(range(0, LONG_RANGE_LEAD_END + 1))
    required = list(range(0, LONG_RANGE_REQUIRED_LEAD_END + 1))
    contiguous = leads == list(range(leads[0], leads[-1] + 1)) if leads else False
    usable_background = bool(leads) and leads[0] == 0 and contiguous and leads[-1] >= LONG_RANGE_LEAD_START
    missing_required = [lead for lead in required if not daily_by_lead.get(lead)]
    if contiguous and leads and leads[0] == 0 and not missing_required:
        status = "PASS"
    elif usable_background:
        status = "PARTIAL"
    else:
        status = "FAIL"
    return {
        "status": status,
        "expected_lead_day_range": [0, LONG_RANGE_LEAD_END],
        "required_lead_day_range": [0, LONG_RANGE_REQUIRED_LEAD_END],
        "actual_lead_day_range": [leads[0], leads[-1]] if leads else None,
        "actual_lead_days": len(leads),
        "contiguous": contiguous,
        "usable_background_through_lead_day": leads[-1] if usable_background else None,
        "missing_lead_days": [lead for lead in expected if lead not in daily_by_lead],
        "missing_required_lead_days": missing_required,
        "edge_shortfall_lead_days": [
            lead for lead in expected
            if lead > LONG_RANGE_REQUIRED_LEAD_END and not daily_by_lead.get(lead)
        ],
        "required_forecast_days": LONG_RANGE_REQUIRED_LEAD_END + 1,
        "method": (
            "requires contiguous lead days 0..%d; lead days %d..%d belong to the trailing 3-day block "
            "and are published only by the freshest long run" % (
                LONG_RANGE_REQUIRED_LEAD_END,
                LONG_RANGE_REQUIRED_LEAD_END + 1,
                LONG_RANGE_LEAD_END,
            )
        ),
    }


def long_range_variable_horizon_offenders(
    member_check: dict,
    daily_by_lead: dict[int, dict[str, dict]],
) -> list[str]:
    """Variables truncated inside the required horizon rather than on the model edge.

    `ncep_gefs05` routinely publishes precipitation and snowfall one 3-day block
    shorter than temperature, so those variables are commonly edge truncated while
    temperature still reaches the trailing block.  That is a property of the product,
    not a data failure: only a variable whose common complete range stops (or starts)
    inside the required D0..D33 range may downgrade the region.
    """
    member_check = member_check or {}
    availability = member_check.get("variable_availability") or {}
    truncated = list(member_check.get("edge_truncated_variables") or [])
    if not truncated:
        return []
    dates_by_lead: dict[int, str] = {}
    for lead, members in (daily_by_lead or {}).items():
        for day in members.values():
            if isinstance(day, dict) and day.get("date"):
                dates_by_lead[lead] = day["date"]
                break
    if not dates_by_lead:
        return truncated
    origin = dt.date.fromisoformat(dates_by_lead[min(dates_by_lead)])
    offenders = []
    for variable in truncated:
        item = availability.get(variable) or {}
        first = item.get("first_timestamp")
        last = item.get("last_timestamp")
        if isinstance(first, str) and first and parse_local_api_time(first).date() != origin:
            offenders.append(variable)
            continue
        if isinstance(last, str) and last:
            if (parse_local_api_time(last).date() - origin).days < LONG_RANGE_REQUIRED_LEAD_END:
                offenders.append(variable)
    return offenders


def long_range_member_window_values(
    daily_by_lead: dict[int, dict[str, dict]],
    start_lead: int,
    end_lead: int,
) -> dict[str, dict[str, object]]:
    leads = list(range(start_lead, end_lead + 1))
    member_keys = sorted({key for lead in leads for key in daily_by_lead.get(lead, {})})
    values = {}
    for member_key in member_keys:
        days = [daily_by_lead.get(lead, {}).get(member_key) for lead in leads]
        if any(day is None for day in days):
            continue
        def complete_metric(key: str, operation: str) -> float | None:
            metric_values = [day.get(key) for day in days]
            if any(value is None for value in metric_values):
                return None
            if operation == "sum":
                return round(sum(metric_values), 3)
            return round(max(metric_values), 3)

        values[member_key] = {
            "temperature_mean_c": round(mean(day["temperature_mean_c"] for day in days), 3),
            "temperature_min_c": round(min(day["temperature_min_c"] for day in days), 3),
            "precipitation_mm": complete_metric("precipitation_mm", "sum"),
            "snowfall_cm": complete_metric("snowfall_cm", "sum"),
            "wind_gust_max_kmh": complete_metric("wind_gust_max_kmh", "max"),
        }
    return values


def signal_from_support(support: float | None) -> str:
    if support is None:
        return "UNDETERMINED"
    if support >= 0.7:
        return "STRONG"
    if support >= 0.4:
        return "MODERATE"
    if support >= 0.2:
        return "WEAK"
    return "NONE"


def support_for_window_metric(
    member_values: dict[str, dict[str, object]],
    key: str,
    predicate,
) -> tuple[float | None, int]:
    available = [item.get(key) for item in member_values.values() if item.get(key) is not None]
    if not available:
        return None, 0
    return round(sum(predicate(float(value)) for value in available) / len(available), 3), len(available)


def temperature_background(temperature_stats: dict, reference_mean: float | None) -> dict:
    if reference_mean is None or temperature_stats.get("mean") is None:
        return {
            "direction": "UNDETERMINED",
            "strength": "WEAK",
            "reference_status": "UNAVAILABLE",
        }
    delta = float(temperature_stats["mean"]) - reference_mean
    if abs(delta) < 0.5:
        direction = "NEAR_REFERENCE"
    elif delta < 0:
        direction = "COLDER_THAN_REFERENCE"
    else:
        direction = "WARMER_THAN_REFERENCE"
    strength = "STRONG" if abs(delta) >= 3 else "MODERATE" if abs(delta) >= 1.5 else "WEAK"
    return {
        "direction": direction,
        "strength": strength,
        "reference_status": "PASS",
    }


def long_range_temperature_stats(member_values: dict[str, dict[str, object]]) -> dict:
    values = [float(item["temperature_mean_c"]) for item in member_values.values()]
    return ensemble_statistics(values)


def long_range_cold_window_signal(
    current_values: dict[str, dict[str, object]],
    previous_values: dict[str, dict[str, object]],
    start_date: str,
    end_date: str,
) -> tuple[dict, dict]:
    common_members = sorted(set(current_values) & set(previous_values))
    if not common_members:
        return (
            {
                "signal": "UNDETERMINED",
                "window": f"{start_date}/{end_date}",
                "member_support": None,
                "persistence_runs": 0,
            },
            {"status": "NO_ROBUST_SIGNAL", "reason": "NO_PREVIOUS_WINDOW_DATA"},
        )
    changes = [
        float(current_values[key]["temperature_mean_c"])
        - float(previous_values[key]["temperature_mean_c"])
        for key in common_members
    ]
    member_support = sum(change <= -1.0 for change in changes) / len(changes)
    current_stats = long_range_temperature_stats({key: current_values[key] for key in common_members})
    previous_stats = long_range_temperature_stats({key: previous_values[key] for key in common_members})
    mean_change = current_stats["mean"] - previous_stats["mean"]
    median_change = current_stats["median"] - previous_stats["median"]
    spread = current_stats["spread"] or 0
    if mean_change <= -3 and median_change <= -2 and member_support >= 0.7 and spread <= 8:
        signal = "STRONG"
    elif mean_change <= -1.5 and median_change <= -1 and member_support >= 0.6 and spread <= 10:
        signal = "MODERATE"
    elif mean_change <= -0.8 and member_support >= 0.5 and spread <= 12:
        signal = "WEAK"
    else:
        signal = "NONE"
    return (
        {
            "signal": signal,
            "window": f"{start_date}/{end_date}",
            "member_support": round(member_support, 3),
            "persistence_runs": 0,
        },
        {
            "status": "ASSESSED",
            "mean_change_c": round(mean_change, 3),
            "median_change_c": round(median_change, 3),
            "spread_c": spread,
            "member_count": len(common_members),
        },
    )


def long_range_uncertainty(
    temperature_stats: dict,
    event_supports: list[float | None],
    start_lead: int,
) -> tuple[str, list[str]]:
    level = 0
    drivers = []
    spread = temperature_stats.get("spread")
    if spread is None:
        level = 3
        drivers.append("temperature_spread_unavailable")
    elif spread > 10:
        level = max(level, 3)
        drivers.append("temperature_spread")
    elif spread > 7:
        level = max(level, 2)
        drivers.append("temperature_spread")
    elif spread > 4:
        level = max(level, 1)
        drivers.append("temperature_spread")
    disagreement = [support for support in event_supports if support is not None and 0.25 <= support <= 0.75]
    if disagreement:
        level = max(level, 2)
        drivers.append("member_event_disagreement")
    if any(support is None for support in event_supports):
        level = max(level, 2)
        drivers.append("event_variable_horizon")
    if start_lead >= 28:
        level = max(level, 2)
        drivers.append("longer_lead_time")
    if not drivers:
        drivers.append("long_range_horizon")
    return LONG_RANGE_UNCERTAINTY_ORDER[level], drivers


def reference_mean_for_window(
    reference_days: dict[str, dict],
    origin_date: dt.date,
    start_lead: int,
    end_lead: int,
) -> tuple[float | None, int]:
    values = []
    for lead in range(start_lead, end_lead + 1):
        target = origin_date + dt.timedelta(days=lead)
        reference = reference_days.get(f"2025-{target.month:02d}-{target.day:02d}")
        if reference and reference.get("complete") and metric_value(reference, "temperature_mean_c") is not None:
            values.append(metric_value(reference, "temperature_mean_c"))
    return (round(mean(values), 3), len(values)) if values else (None, 0)


def build_long_range_windows(
    origin_date: dt.date,
    daily_by_lead: dict[int, dict[str, dict]],
    reference_days: dict[str, dict],
    max_date: dt.date | None = None,
) -> list[dict]:
    windows = []
    for start_lead, end_lead in long_range_window_definitions():
        effective_end_lead = end_lead
        if max_date is not None:
            max_lead = (max_date - origin_date).days
            if max_lead < start_lead:
                continue
            effective_end_lead = min(end_lead, max_lead)
        current_values = long_range_member_window_values(daily_by_lead, start_lead, effective_end_lead)
        start_date = origin_date + dt.timedelta(days=start_lead)
        end_date = origin_date + dt.timedelta(days=effective_end_lead)
        if not current_values:
            windows.append({
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "horizon_class": f"D{start_lead}_D{effective_end_lead}",
                "requested_horizon_class": f"D{start_lead}_D{end_lead}",
                "confidence": "VERY_LOW",
                "status": "UNAVAILABLE",
                "reason": "WINDOW_DATA_MISSING",
            })
            continue
        temperature_stats = long_range_temperature_stats(current_values)
        reference_mean, reference_days_available = reference_mean_for_window(
            reference_days, origin_date, start_lead, effective_end_lead
        )
        previous_values = long_range_member_window_values(daily_by_lead, start_lead - 3, start_lead - 1)
        cold_signal, cold_diagnostics = long_range_cold_window_signal(
            current_values,
            previous_values,
            start_date.isoformat(),
            end_date.isoformat(),
        )
        precipitation_support, precipitation_members = support_for_window_metric(
            current_values,
            "precipitation_mm",
            lambda value: value >= 1,
        )
        snowfall_support, snowfall_members = support_for_window_metric(
            current_values,
            "snowfall_cm",
            lambda value: value > 0.1,
        )
        wind_support, wind_members = support_for_window_metric(
            current_values,
            "wind_gust_max_kmh",
            lambda value: value >= 50,
        )
        wet_snow_values = [
            item
            for item in current_values.values()
            if item.get("snowfall_cm") is not None and item.get("temperature_min_c") is not None
        ]
        wet_snow_members = len(wet_snow_values)
        wet_snow_support = (
            round(
                sum(
                    float(item["snowfall_cm"]) > 0.1 and float(item["temperature_min_c"]) <= 2
                    for item in wet_snow_values
                ) / wet_snow_members,
                3,
            )
            if wet_snow_values
            else None
        )
        coarse_thresholds = {}
        for threshold in (5.0, 2.0, 0.0):
            support = sum(
                float(item["temperature_min_c"]) < threshold
                for item in current_values.values()
            ) / len(current_values)
            coarse_thresholds[f"below_{int(threshold)}c"] = {
                "member_support": round(support, 3),
                "threshold_c": threshold,
                "definition": "at least one coarse-grid daily minimum in this 3-day window",
            }
        uncertainty, uncertainty_drivers = long_range_uncertainty(
            temperature_stats,
            [precipitation_support, snowfall_support, wind_support, wet_snow_support],
            start_lead,
        )
        confidence = "VERY_LOW" if uncertainty in {"HIGH", "VERY_HIGH"} or start_lead >= 28 else "LOW"
        windows.append({
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "horizon_class": f"D{start_lead}_D{effective_end_lead}",
            "requested_horizon_class": f"D{start_lead}_D{end_lead}",
            "confidence": confidence,
            "status": "OK",
            "temperature_distribution_c": temperature_stats,
            "temperature_background": temperature_background(temperature_stats, reference_mean),
            "historical_reference": {
                "kind": "historical_reference",
                "model": "ECMWF IFS 9 km historical weather / analysis",
                "mean_temperature_c": reference_mean,
                "days_available": reference_days_available,
            },
            "cold_window_signal": cold_signal,
            "precipitation_background": {
                "signal": signal_from_support(precipitation_support),
                "member_support": precipitation_support,
                "available_members": precipitation_members,
                "threshold": "window precipitation total >= 1 mm",
            },
            "snow_background": {
                "signal": signal_from_support(snowfall_support),
                "member_support": snowfall_support,
                "available_members": snowfall_members,
                "threshold": "window snowfall total > 0.1 cm",
            },
            "wet_snow_assessment": {
                "status": "UNAVAILABLE" if wet_snow_support is None else "COARSE_POTENTIAL" if wet_snow_support else "NO_SIGNAL",
                "member_support": wet_snow_support,
                "available_members": wet_snow_members,
                "reason": "coarse 0.5 degree member overlap of snowfall and <=2C daily minimum; not local phase certainty",
            },
            "coarse_grid_threshold_signal": {
                "usable_for_local_absolute_temperature": False,
                "thresholds": coarse_thresholds,
                "reason": "0.5 degree ensemble grid cannot represent point-level absolute temperature",
            },
            "strong_wind_background": {
                "signal": signal_from_support(wind_support),
                "member_support": wind_support,
                "available_members": wind_members,
                "threshold": "window maximum gust >= 50 km/h",
            },
            "forecast_uncertainty": uncertainty,
            "uncertainty_drivers": uncertainty_drivers,
            "diagnostics": {
                "cold_window": cold_diagnostics,
                "member_count": len(current_values),
            },
            "signal_evolution": {
                "status": "INSUFFICIENT_HISTORY",
                "runs_seen": 0,
                "trend": "UNDETERMINED",
            },
        })
    return windows


def load_long_range_snapshots(
    current_date: dt.date,
    limit: int = 5,
    namespace: str | None = None,
) -> list[dict]:
    snapshots = []
    if not ARCHIVE_DIR.exists():
        return snapshots
    pattern = f"*/{namespace}/long_range.json" if namespace else "*/long_range.json"
    for path in ARCHIVE_DIR.glob(pattern):
        archive_date_path = path.parent.parent if namespace else path.parent
        try:
            archive_date = dt.date.fromisoformat(archive_date_path.name)
        except ValueError:
            continue
        if archive_date >= current_date:
            continue
        try:
            with path.open(encoding="utf-8") as handle:
                value = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue
        if value.get("status") in {"OK", "PARTIAL"}:
            snapshots.append(value)
    snapshots.sort(key=lambda value: value.get("generated_at", ""), reverse=True)
    return snapshots[:limit]


def apply_signal_evolution(region_id: str, windows: list[dict], snapshots: list[dict]) -> list[dict]:
    for window in windows:
        if window.get("status") == "UNAVAILABLE" or "cold_window_signal" not in window:
            window["signal_evolution"] = {
                "status": "INSUFFICIENT_HISTORY",
                "runs_seen": 0,
                "trend": "UNDETERMINED",
            }
            continue
        current_signal = window.get("cold_window_signal", {}).get("signal")
        matches = []
        for snapshot in snapshots:
            region = (snapshot.get("regions") or {}).get(region_id) or {}
            match = next(
                (item for item in region.get("windows", []) if item.get("horizon_class") == window.get("horizon_class")),
                None,
            )
            if match:
                matches.append(match)
        previous_signals = [item.get("cold_window_signal", {}).get("signal") for item in matches]
        previous_signal = previous_signals[0] if previous_signals else None
        persistence_runs = 1 if current_signal in {"WEAK", "MODERATE", "STRONG"} else 0
        for signal in previous_signals:
            if signal in {"WEAK", "MODERATE", "STRONG"} and persistence_runs:
                persistence_runs += 1
            else:
                break
        if not matches:
            evolution_status = "INSUFFICIENT_HISTORY"
            trend = "UNDETERMINED"
        elif current_signal == "NONE" and previous_signal in {"WEAK", "MODERATE", "STRONG"}:
            evolution_status = "DISAPPEARED"
            trend = "WEAKENING"
        elif current_signal in {"WEAK", "MODERATE", "STRONG"} and previous_signal not in {"WEAK", "MODERATE", "STRONG"}:
            evolution_status = "NEW"
            trend = "STRENGTHENING"
        elif current_signal not in LONG_RANGE_SIGNAL_ORDER or previous_signal not in LONG_RANGE_SIGNAL_ORDER:
            evolution_status = "INSUFFICIENT_HISTORY"
            trend = "UNDETERMINED"
        else:
            current_rank = LONG_RANGE_SIGNAL_ORDER.index(current_signal) if current_signal in LONG_RANGE_SIGNAL_ORDER else 0
            previous_rank = LONG_RANGE_SIGNAL_ORDER.index(previous_signal) if previous_signal in LONG_RANGE_SIGNAL_ORDER else 0
            current_start = dt.date.fromisoformat(window["start_date"])
            previous_start = dt.date.fromisoformat(matches[0]["start_date"])
            if abs((current_start - previous_start).days) > 1:
                evolution_status = "SHIFTING"
                trend = "SHIFTING"
            elif current_rank > previous_rank:
                evolution_status = "STRENGTHENING"
                trend = "STRENGTHENING"
            elif current_rank < previous_rank:
                evolution_status = "WEAKENING"
                trend = "WEAKENING"
            else:
                evolution_status = "PERSISTENT" if current_signal != "NONE" else "INSUFFICIENT_HISTORY"
                trend = "STABLE" if evolution_status == "PERSISTENT" else "UNDETERMINED"
        window["cold_window_signal"]["persistence_runs"] = persistence_runs
        window["signal_evolution"] = {
            "status": evolution_status,
            "runs_seen": len(matches) + 1,
            "trend": trend,
        }
        if evolution_status == "SHIFTING":
            uncertainty = window.get("forecast_uncertainty", "VERY_HIGH")
            current_level = LONG_RANGE_UNCERTAINTY_ORDER.index(uncertainty) if uncertainty in LONG_RANGE_UNCERTAINTY_ORDER else 3
            upgraded_level = min(3, max(2, current_level + 1))
            window["forecast_uncertainty"] = LONG_RANGE_UNCERTAINTY_ORDER[upgraded_level]
            window.setdefault("uncertainty_drivers", []).append("run_to_run_shift")
    return windows


def highest_signal(windows: list[dict], path: tuple[str, ...]) -> str:
    values = []
    for window in windows:
        value: object = window
        for key in path:
            value = value.get(key) if isinstance(value, dict) else None
        if value in LONG_RANGE_SIGNAL_ORDER:
            values.append(value)
    return max(values, key=LONG_RANGE_SIGNAL_ORDER.index) if values else "UNDETERMINED"


def long_range_overall(windows: list[dict]) -> dict:
    directions = [
        window.get("temperature_background", {}).get("direction")
        for window in windows
        if window.get("temperature_background", {}).get("direction") in {
            "COLDER_THAN_REFERENCE",
            "NEAR_REFERENCE",
            "WARMER_THAN_REFERENCE",
        }
    ]
    if directions:
        counts = {value: directions.count(value) for value in set(directions)}
        top_count = max(counts.values())
        top_directions = [value for value, count in counts.items() if count == top_count]
        direction = top_directions[0] if len(top_directions) == 1 else "UNDETERMINED"
    else:
        direction = "UNDETERMINED"
    notable = []
    for window in windows:
        signals = []
        if window.get("cold_window_signal", {}).get("signal") in {"WEAK", "MODERATE", "STRONG"}:
            signals.append("COLD_WINDOW")
        if window.get("snow_background", {}).get("signal") in {"WEAK", "MODERATE", "STRONG"}:
            signals.append("SNOW_BACKGROUND")
        if window.get("precipitation_background", {}).get("signal") in {"WEAK", "MODERATE", "STRONG"}:
            signals.append("PRECIPITATION_BACKGROUND")
        if window.get("strong_wind_background", {}).get("signal") in {"WEAK", "MODERATE", "STRONG"}:
            signals.append("STRONG_WIND_BACKGROUND")
        if signals:
            notable.append({
                "start_date": window["start_date"],
                "end_date": window["end_date"],
                "signals": signals,
            })
    uncertainty = max(
        (window.get("forecast_uncertainty") for window in windows if window.get("forecast_uncertainty") in LONG_RANGE_UNCERTAINTY_ORDER),
        key=LONG_RANGE_UNCERTAINTY_ORDER.index,
        default="VERY_HIGH",
    )
    return {
        "temperature": {"direction": direction},
        "cold_air": {"signal": highest_signal(windows, ("cold_window_signal", "signal"))},
        "moisture": {"signal": highest_signal(windows, ("precipitation_background", "signal"))},
        "snow": {"signal": highest_signal(windows, ("snow_background", "signal"))},
        "wind": {"signal": highest_signal(windows, ("strong_wind_background", "signal"))},
        "confidence": "VERY_LOW" if uncertainty in {"HIGH", "VERY_HIGH"} else "LOW",
        "uncertainty": uncertainty,
        "notable_windows": notable,
    }


def run_long_range_reference(
    config: dict,
    point: dict,
    client: ApiClient,
    origin_date: dt.date,
    generated_at: str,
    data_date: str,
    *,
    refresh_history: bool = False,
    cache_dir: Path | None = None,
) -> dict:
    start_date = origin_date + dt.timedelta(days=LONG_RANGE_LEAD_START)
    end_date = origin_date + dt.timedelta(days=LONG_RANGE_LEAD_END)
    try:
        reference_start = dt.date(2025, start_date.month, start_date.day)
        reference_end = dt.date(2025, end_date.month, end_date.day)
    except ValueError as error:
        return {
            "status": "FAILED",
            "reason": f"REFERENCE_DATE_INVALID:{error}",
            "daily": [],
            "record": None,
        }
    record = history_cache_record_or_fetch(
        config,
        client,
        point,
        2025,
        reference_start.isoformat(),
        reference_end.isoformat(),
        refresh_history=refresh_history,
        cache_dir=cache_dir,
        log_label=f"{point['id']}:LONG_REFERENCE 2025",
    )
    if record.get("status") == "PASS":
        log(f"[{point['id']}] LONG_REFERENCE 2025 OK")
    return {
        "status": "PASS" if record.get("status") == "PASS" else "FAILED",
        "daily": record.get("daily", []),
        "record": record,
        "period_start": reference_start.isoformat(),
        "period_end": reference_end.isoformat(),
    }


def run_long_range(
    config: dict,
    client: ApiClient,
    generated_at: str,
    data_date: str,
    now_local: dt.datetime,
    archive_namespace: str | None = None,
    refresh_history: bool = False,
    cache_dir: Path | None = None,
) -> dict:
    points = active_points(config)
    forecast_records: dict[str, dict] = {}
    raw_references: dict[str, dict] = {}
    active_core_ids = []
    for region_id in core_region_ids(config):
        core_id = config["regions"][region_id].get("core_point_id")
        point = points.get(core_id) if core_id else None
        if not point:
            log(f"[{region_id}] LONG_RANGE SKIPPED: PROVISIONAL")
            continue
        active_core_ids.append(core_id)
        record = fetch_point(
            client,
            point=point,
            source="Open-Meteo",
            endpoint=OPEN_METEO_ENDPOINTS["ensemble"],
            model=LONG_RANGE_MODEL,
            params=base_weather_params(
                point,
                models=LONG_RANGE_MODEL_ID,
                forecast_days=LONG_RANGE_REQUESTED_FORECAST_DAYS,
            ),
            variables=LONG_RANGE_VARIABLES,
            # Temperature is the core horizon signal. Other event variables may
            # legitimately end earlier; their per-variable edge availability is
            # audited and their late windows remain UNDETERMINED.
            required_variables=["temperature_2m"],
            grid_limit_km=LONG_RANGE_GRID_QA_LIMIT_KM,
            log_label=f"{core_id}:LONG_RANGE",
            accepted_model_values=(LONG_RANGE_MODEL_ID, LONG_RANGE_MODEL),
            accepted_model_ids=(LONG_RANGE_MODEL_ID,),
            max_forecast_date=point_forecast_end_date(point),
        )
        if record.get("status") == "PASS":
            record.setdefault("qa", {})["grid_scale_class"] = "coarse_ensemble"
            record = trim_long_range_member_edges(record)
            members_valid, member_check = validate_long_range_members(record)
            record.setdefault("qa", {})["long_range_member_check"] = member_check
            if not members_valid:
                record["status"] = "INVALID"
                record["qa"]["valid"] = False
                record["qa"]["final_status"] = "INVALID"
                record["qa"]["reason"] = "LONG_RANGE_MEMBER_SCHEMA_INVALID"
                log(f"[{core_id}] LONG_RANGE MEMBER QA FAIL")
        forecast_records[region_id] = record

    snapshots = load_long_range_snapshots(now_local.date(), namespace=archive_namespace)
    regions = {}
    successful_points = 0
    partial_points = 0
    failed_points = 0
    history_cache_info = history_cache_stats()
    forecast_horizons = []
    member_counts = []
    for region_id in core_region_ids(config):
        region_config = config["regions"][region_id]
        core_id = region_config.get("core_point_id")
        point = points.get(core_id) if core_id else None
        if not point:
            regions[region_id] = {
                "status": "UNAVAILABLE",
                "usable_for_main_chain": False,
                "reason": "NO_VERIFIED_CORE_POINT",
                "windows": [],
                "overall_16_35d": {"temperature": {"direction": "UNDETERMINED"}, "confidence": "VERY_LOW"},
                "qa": {"final_status": "UNAVAILABLE", "reason": "NO_VERIFIED_CORE_POINT"},
            }
            continue
        record = forecast_records.get(region_id) or {}
        if record.get("status") != "PASS":
            failed_points += 1
            regions[region_id] = {
                "status": "FAILED",
                "usable_for_main_chain": True,
                "point_id": core_id,
                "visit_date": region_config.get("primary_visit_date"),
                "windows": [],
                "overall_16_35d": {"temperature": {"direction": "UNDETERMINED"}, "confidence": "VERY_LOW"},
                "reason": (record.get("qa") or {}).get("reason", "OPEN_METEO_LONG_RANGE_NOT_AVAILABLE"),
                "qa": record.get("qa") or {"final_status": "INVALID", "reason": "OPEN_METEO_LONG_RANGE_NOT_AVAILABLE"},
            }
            continue
        origin_date, daily_by_lead = long_range_daily_member_values(record["hourly"])
        horizon_check = long_range_horizon_check(daily_by_lead)
        record.setdefault("qa", {})["long_range_horizon_check"] = horizon_check
        if horizon_check["status"] == "FAIL":
            record["status"] = "INVALID"
            record["qa"]["valid"] = False
            record["qa"]["final_status"] = "INVALID"
            record["qa"]["reason"] = "LONG_RANGE_HORIZON_INVALID"
            failed_points += 1
            regions[region_id] = {
                "status": "FAILED",
                "usable_for_main_chain": True,
                "point_id": core_id,
                "visit_date": region_config.get("primary_visit_date"),
                "windows": [],
                "overall_16_35d": {"temperature": {"direction": "UNDETERMINED"}, "confidence": "VERY_LOW"},
                "reason": "LONG_RANGE_HORIZON_INVALID",
                "qa": record.get("qa"),
            }
            continue
        successful_points += 1
        last_time = (record.get("hourly") or {}).get("time", [None])[-1]
        forecast_horizons.append(horizon_check["actual_lead_days"])
        member_check = record.get("qa", {}).get("long_range_member_check", {})
        member_counts.extend(member_check.get("actual_member_counts_by_variable", {}).values())
        variable_horizon_offenders = long_range_variable_horizon_offenders(member_check, daily_by_lead)
        variable_horizon_partial = bool(variable_horizon_offenders)
        reference = run_long_range_reference(
            config,
            point,
            client,
            origin_date,
            generated_at,
            data_date,
            refresh_history=refresh_history,
            cache_dir=cache_dir,
        )
        if reference.get("record"):
            update_history_cache_stats(history_cache_info, reference["record"])
        raw_references[region_id] = reference.get("record")
        reference_days = {day["date"]: day for day in reference.get("daily", [])}
        windows = build_long_range_windows(
            origin_date,
            daily_by_lead,
            reference_days,
            max_date=point_forecast_end_date(point),
        )
        windows = apply_signal_evolution(region_id, windows, snapshots)
        region_status = "OK" if (
            reference.get("status") == "PASS"
            and horizon_check["status"] == "PASS"
            and not variable_horizon_partial
        ) else "PARTIAL"
        if region_status == "PARTIAL":
            partial_points += 1
        regions[region_id] = {
            "status": region_status,
            "usable_for_main_chain": True,
            "point_id": core_id,
            "visit_date": region_config.get("primary_visit_date"),
            "forecast_origin_date": origin_date.isoformat(),
            "forecast_last_timestamp": last_time,
            "windows": windows,
            "overall_16_35d": long_range_overall(windows),
            "historical_reference": {
                "status": reference.get("status"),
                "period_start": reference.get("period_start"),
                "period_end": reference.get("period_end"),
                "kind": "historical_reference",
            },
            "qa": {
                "forecast": record.get("qa"),
                "request_coordinate": (record.get("request") or {}).get("coordinate"),
                "returned_grid_coordinate": (record.get("response") or {}).get("grid_coordinate"),
                "returned_elevation": (record.get("response") or {}).get("returned_elevation"),
                "historical_reference": (reference.get("record") or {}).get("qa"),
                "grid_scale_class": "coarse_ensemble",
                "variable_horizon_partial": variable_horizon_partial,
                "variable_horizon_partial_variables": variable_horizon_offenders,
                "edge_truncated_variables": list(member_check.get("edge_truncated_variables") or []),
                "final_status": "PASS" if region_status == "OK" else "PARTIAL",
            },
        }

    for region_id, region_config in config["regions"].items():
        if region_id in regions:
            continue
        log(f"[{region_id}] LONG_RANGE SKIPPED: PROVISIONAL")
        regions[region_id] = {
            "status": "UNAVAILABLE",
            "usable_for_main_chain": False,
            "reason": "NO_VERIFIED_CORE_POINT",
            "windows": [],
            "overall_16_35d": {"temperature": {"direction": "UNDETERMINED"}, "confidence": "VERY_LOW"},
            "qa": {"final_status": "UNAVAILABLE", "reason": "NO_VERIFIED_CORE_POINT"},
        }

    if not active_core_ids or failed_points == len(active_core_ids):
        status = "FAILED"
    elif failed_points or partial_points:
        status = "PARTIAL"
    else:
        status = "OK"
    actual_horizon = min(forecast_horizons) if forecast_horizons else None
    actual_members = min(member_counts) if member_counts else None
    required_delivery_days = LONG_RANGE_REQUIRED_LEAD_END + 1
    horizon_status = (
        "PASS"
        if actual_horizon is not None and actual_horizon >= required_delivery_days
        else "PARTIAL"
        if actual_horizon and actual_horizon > LONG_RANGE_LEAD_START
        else "FAILED"
    )
    return module_header(
        "long_range_background",
        generated_at,
        data_date,
        status,
        source="Open-Meteo",
        endpoint=OPEN_METEO_ENDPOINTS["ensemble"],
        model=LONG_RANGE_MODEL,
        model_id=LONG_RANGE_MODEL_ID,
        model_documentation=LONG_RANGE_ENDPOINT_DOC,
        model_registry_documentation=LONG_RANGE_MODEL_REGISTRY_DOC,
        ensemble_members=actual_members or LONG_RANGE_ENSEMBLE_MEMBERS,
        expected_ensemble_members=LONG_RANGE_ENSEMBLE_MEMBERS,
        requested_forecast_days=LONG_RANGE_REQUESTED_FORECAST_DAYS,
        required_forecast_days=required_delivery_days,
        forecast_horizon_days=actual_horizon,
        forecast_lead_days=max(0, (actual_horizon or 1) - 1),
        forecast_horizon_status=horizon_status,
        forecast_last_timestamp=max(
            (region.get("forecast_last_timestamp") for region in regions.values() if region.get("forecast_last_timestamp")),
            default=None,
        ),
        native_resolution="0.5° (~50 km)",
        native_time_resolution="3-hourly",
        time_resolution_note="Open-Meteo documents hourly interpolation for ensemble output; the GFS 0.5° product is natively 3-hourly.",
        coverage="global; western Sichuan is within the global product domain",
        aggregation={
            "type": "fixed_3_day_blocks",
            "lead_day_range": f"D{LONG_RANGE_LEAD_START}_D{LONG_RANGE_LEAD_END}",
            "required_lead_day_range": f"D{LONG_RANGE_LEAD_START}_D{LONG_RANGE_REQUIRED_LEAD_END}",
            "edge_blocks": [
                f"D{start}_D{end}"
                for start, end in long_range_window_definitions()
                if end <= LONG_RANGE_LEAD_END and end > LONG_RANGE_REQUIRED_LEAD_END
            ],
            "windows": [f"D{start}_D{end}" for start, end in long_range_window_definitions()],
            "hourly_values_in_public_artifact": False,
        },
        interpretation_boundary=config.get(
            "long_range_interpretation_boundary",
            "16-35 day background signal only; not a date-level precise forecast and not a direct phenology lead/lag calculation.",
        ),
        regions=regions,
        excluded_points=excluded_points(config),
        raw_references=raw_references,
        raw_points=forecast_records,
        history_cache={
            "enabled": True,
            "directory": history_cache_relative_path(Path(cache_dir) if cache_dir is not None else HISTORY_CACHE_DIR),
            "refresh_requested": refresh_history,
            **history_cache_info,
        },
        successful_points=successful_points,
        partial_points=partial_points,
        failed_points=failed_points,
        qa={
            "grid_scale_class": "coarse_ensemble",
            "expected_ensemble_members": LONG_RANGE_ENSEMBLE_MEMBERS,
            "actual_ensemble_members": actual_members,
            "actual_horizon_days_including_today": actual_horizon,
            "horizon_status": horizon_status,
            "forecast_lead_days": max(0, (actual_horizon or 1) - 1),
            "last_available_timestamp": max(
                (region.get("forecast_last_timestamp") for region in regions.values() if region.get("forecast_last_timestamp")),
                default=None,
            ),
            "region_statuses": {region_id: region.get("status") for region_id, region in regions.items()},
        },
    )


def select_single_run_target(region_config: dict, now_local: dt.datetime) -> tuple[str, str, str | None]:
    raw_visit_date = region_config.get("primary_visit_date")
    visit_date = dt.date.fromisoformat(raw_visit_date) if raw_visit_date else None
    if visit_date:
        visit_target = dt.datetime.combine(visit_date, dt.time(5, 0), tzinfo=LOCAL_TZ)
        days_ahead = (visit_target - now_local).total_seconds() / 86400
        if 0 <= days_ahead <= 9:
            return (
                visit_target.strftime("%Y-%m-%dT%H:%M"),
                "primary_visit_date_within_10_day_run_horizon",
                visit_date.isoformat(),
            )
    rolling_date = now_local.date() + dt.timedelta(days=3)
    raw_cutoff = region_config.get("forecast_end_date")
    if raw_cutoff:
        rolling_date = min(rolling_date, dt.date.fromisoformat(str(raw_cutoff)))
    rolling = dt.datetime.combine(rolling_date, dt.time(5, 0), tzinfo=LOCAL_TZ)
    policy = "rolling_plus_3_days_before_visit_window" if visit_date else "rolling_plus_3_days_no_visit_date"
    return rolling.strftime("%Y-%m-%dT%H:%M"), policy, visit_date.isoformat() if visit_date else None


def candidate_single_runs(now_utc: dt.datetime, count: int = 8) -> list[dt.datetime]:
    cycle_hour = (now_utc.hour // 6) * 6
    latest_cycle = now_utc.replace(hour=cycle_hour, minute=0, second=0, microsecond=0)
    latest_available_estimate = latest_cycle - dt.timedelta(hours=6)
    return [latest_available_estimate - dt.timedelta(hours=6 * index) for index in range(count)]


def target_values(record: dict, target_time: str) -> dict:
    hourly = record.get("hourly") or {}
    times = hourly.get("time") or []
    try:
        index = times.index(target_time)
    except ValueError:
        return {"status": "INVALID", "reason": "TARGET_TIME_NOT_IN_RUN"}
    values = {}
    for variable in SINGLE_RUN_VARIABLES:
        actual_variable = record.get("solar_variable") if variable == "sunshine_duration" else variable
        if not actual_variable:
            continue
        series = hourly.get(actual_variable) or []
        values[actual_variable] = series[index] if index < len(series) else None
    if any(value is None for value in values.values()):
        return {"status": "INVALID", "reason": "TARGET_VALUE_MISSING", "values": values}
    return {"status": "PASS", "time": target_time, "values": values}


def single_run_cycle_class(init_time: dt.datetime) -> str:
    """Classify an ECMWF cycle as a full-horizon run or a short run."""
    return "LONG" if init_time.hour in SINGLE_RUN_LONG_CYCLE_HOURS else "SHORT"


def run_single_runs(config: dict, client: ApiClient, generated_at: str, data_date: str, now_utc: dt.datetime) -> dict:
    points = active_points(config)
    now_local = now_utc.astimezone(LOCAL_TZ)
    regions: dict[str, dict] = {}
    successful_run_entries = []
    candidates = candidate_single_runs(now_utc, 8)
    required_candidates = [value for value in candidates if single_run_cycle_class(value) == "LONG"]
    short_candidates = [value for value in candidates if single_run_cycle_class(value) == "SHORT"]
    for region_id in core_region_ids(config):
        region_config = config["regions"][region_id]
        core_id = region_config.get("core_point_id")
        point = points.get(core_id) if core_id else None
        if not point:
            log(f"[{region_id}] SINGLE_RUNS SKIPPED: PROVISIONAL")
            regions[region_id] = {
                "status": "SKIPPED",
                "usable_for_main_chain": False,
                "visit_date": region_config.get("primary_visit_date"),
                "runs": [],
            }
            continue
        target_time, target_policy, visit_date = select_single_run_target(region_config, now_local)
        run_entries = []
        for candidate in candidates:
            run_param = candidate.strftime("%Y-%m-%dT%H:%M")
            record = fetch_point(
                client,
                point=point,
                source="Open-Meteo",
                endpoint=OPEN_METEO_ENDPOINTS["single_runs"],
                model="ECMWF IFS HRES 9 km",
                params=base_weather_params(point, models="ecmwf_ifs", run=run_param, forecast_days=10),
                variables=SINGLE_RUN_VARIABLES,
                required_variables=SINGLE_RUN_REQUIRED_VARIABLES,
                grid_limit_km=site_hres_grid_qa_limit_km(config),
                log_label=f"{core_id}:SINGLE_RUN {run_param}",
                precision_module="single_runs",
                model_run_initialization=iso_utc(candidate),
                max_forecast_date=point_forecast_end_date(point),
            )
            target = target_values(record, target_time) if record.get("status") == "PASS" else {"status": "INVALID", "reason": "RUN_INVALID"}
            entry_status = "PASS" if record.get("status") == "PASS" and target.get("status") == "PASS" else "INVALID"
            if entry_status == "PASS":
                successful_run_entries.append({"region": region_id, "entry": {"init_time": iso_utc(candidate), "target": target}})
            run_entries.append({
                "init_time": iso_utc(candidate),
                "cycle_class": single_run_cycle_class(candidate),
                "status": entry_status,
                "target_time": target_time,
                "target": target,
                "forecast_horizon_hours": len((record.get("hourly") or {}).get("time") or []),
                "record": record,
            })
        successful = [entry for entry in run_entries if entry["status"] == "PASS"]
        required_entries = [entry for entry in run_entries if entry["cycle_class"] == "LONG"]
        short_entries = [entry for entry in run_entries if entry["cycle_class"] == "SHORT"]
        required_successful = [entry for entry in required_entries if entry["status"] == "PASS"]
        short_successful = [entry for entry in short_entries if entry["status"] == "PASS"]
        latest_change = {"status": "UNDETERMINED", "value_c": None, "latest_init_time": None, "previous_init_time": None}
        if len(successful) >= 2:
            latest_value = successful[0]["target"]["values"].get("temperature_2m")
            previous_value = successful[1]["target"]["values"].get("temperature_2m")
            if isinstance(latest_value, (int, float)) and isinstance(previous_value, (int, float)):
                delta = round(float(latest_value) - float(previous_value), 3)
                latest_change = {
                    "status": "UP" if delta > 0 else "DOWN" if delta < 0 else "NO_CHANGE",
                    "value_c": delta,
                    "latest_init_time": successful[0]["init_time"],
                    "previous_init_time": successful[1]["init_time"],
                    "comparison": "newest_two_successful_runs",
                }
        # Only the 00Z/12Z long cycles are required.  The 06Z/18Z cycles are
        # short runs that structurally cannot reach a target beyond their own
        # horizon, so their absence never marks the region as failed.
        if len(required_successful) == len(required_entries) and required_entries:
            region_status = "OK"
            region_reason = None
        elif len(required_successful) >= SINGLE_RUN_MIN_REQUIRED_RUNS:
            region_status = "PARTIAL"
            region_reason = "SINGLE_RUN_LONG_CYCLE_PARTIALLY_DISTRIBUTED"
        else:
            region_status = "FAILED"
            region_reason = "SINGLE_RUN_LONG_CYCLE_UNAVAILABLE"
        regions[region_id] = {
            "status": region_status,
            "status_reason": region_reason,
            "usable_for_main_chain": True,
            "point_id": core_id,
            "visit_date": visit_date,
            "target_time": f"{target_time}+08:00",
            "target_policy": target_policy,
            "run_count_requested": len(candidates),
            "run_count_available": len(successful),
            "required_run_count_requested": len(required_entries),
            "required_run_count_available": len(required_successful),
            "short_run_count_requested": len(short_entries),
            "short_run_count_available": len(short_successful),
            "target_reachable_by_short_runs": bool(short_successful),
            "latest_change": latest_change,
            "runs": run_entries,
        }
    status_values = [item["status"] for item in regions.values() if item.get("usable_for_main_chain")]
    if not status_values or any(value == "FAILED" for value in status_values):
        module_status = "FAILED"
    elif any(value == "PARTIAL" for value in status_values):
        module_status = "PARTIAL"
    else:
        module_status = "OK"
    return module_header(
        "single_runs",
        generated_at,
        data_date,
        module_status,
        endpoint=OPEN_METEO_ENDPOINTS["single_runs"],
        model="ECMWF IFS HRES 9 km",
        model_id="ecmwf_ifs",
        run_frequency="00, 06, 12, 18 UTC",
        runs_requested=len(candidates),
        runs=[iso_utc(value) for value in candidates],
        required_cycles=f"{SINGLE_RUN_LONG_CYCLE_HOURS[0]:02d}, {SINGLE_RUN_LONG_CYCLE_HOURS[1]:02d} UTC (full horizon)",
        short_cycles=f"{SINGLE_RUN_SHORT_CYCLE_HOURS[0]:02d}, {SINGLE_RUN_SHORT_CYCLE_HOURS[1]:02d} UTC (short runs; not required)",
        required_runs_requested=len(required_candidates),
        short_runs_requested=len(short_candidates),
        min_required_runs=SINGLE_RUN_MIN_REQUIRED_RUNS,
        note=(
            "Run timestamps are UTC initialization times; API availability follows model distribution delay. "
            "Only the 00Z/12Z full-horizon cycles are required for the run-to-run drift comparison; the 06Z/18Z "
            "short cycles cannot reach a target more than about six days out and are reported without penalty."
        ),
        regions=regions,
        excluded_points=excluded_points(config),
    )


def first_days(record: dict, start_index: int, count: int) -> list[dict]:
    return [
        day
        for day in (record.get("daily") or [])[start_index : start_index + count]
        if day.get("complete")
    ]


def forecast_0_7d(days: list[dict]) -> dict:
    selected = days[:7]
    cold_windows = []
    for day in selected:
        night_min = metric_value(day, "night_min_c")
        if night_min is not None and night_min < 5:
            cold_windows.append({
                "date": day["date"],
                "night_min_c": night_min,
                "below_5c": night_min < 5,
                "below_2c": night_min < 2,
                "below_0c": night_min < 0,
                "precision_class": day.get("precision_class"),
            })
    precipitation = [metric_value(day, "precipitation_mm") for day in selected]
    snowfall = [metric_value(day, "snowfall_cm") for day in selected]
    gusts = [metric_value(day, "wind_gust_max_kmh") for day in selected]
    nights = [metric_value(day, "night_min_c") for day in selected]
    precipitation = [value for value in precipitation if value is not None]
    snowfall = [value for value in snowfall if value is not None]
    gusts = [value for value in gusts if value is not None]
    nights = [value for value in nights if value is not None]
    solar = [day.get("solar_metric") for day in selected if isinstance(day.get("solar_metric"), dict)]
    solar_values = [item["value"] for item in solar if isinstance(item.get("value"), (int, float))]
    solar_variable = solar[0].get("variable") if solar else None
    return {
        "window_days": len(selected),
        "cold_windows": cold_windows,
        "frost_signal": {
            "night_count_below_15c": sum(value < 15 for value in nights),
            "night_count_below_10c": sum(value < 10 for value in nights),
            "night_count_below_5c": sum(value < 5 for value in nights),
            "night_count_below_2c": sum(value < 2 for value in nights),
            "night_count_below_0c": sum(value < 0 for value in nights),
            "minimum_night_c": round(min(nights), 3) if nights else None,
        },
        "snow_signal": {
            "days_with_snowfall": sum(value > 0.1 for value in snowfall),
            "maximum_snowfall_cm": round(max(snowfall), 3) if snowfall else None,
            "precipitation_total_mm": round(sum(precipitation), 3) if precipitation else None,
        },
        "wind_signal": {
            "days_gust_ge_35_kmh": sum(value >= 35 for value in gusts),
            "days_gust_ge_50_kmh": sum(value >= 50 for value in gusts),
            "maximum_gust_kmh": round(max(gusts), 3) if gusts else None,
        },
        "cloud_sun_signal": {
            "cloud_cover_mean_pct": safe_mean([value for value in (metric_value(day, "cloud_cover_mean_pct") for day in selected) if value is not None]),
            "cloud_cover_low_mean_pct": safe_mean([value for value in (metric_value(day, "cloud_cover_low_mean_pct") for day in selected) if value is not None]),
            "solar_metric": {
                "variable": solar_variable,
                "mean_value": round(mean(solar_values), 3) if solar_values else None,
                "unit": solar[0].get("unit") if solar else None,
            },
        },
        "daily": selected,
    }


def leaf_loss_weather_risk(days: list[dict], as_of_date: dt.date) -> dict:
    selected = days[:7]
    strong_wind_dates = [
        day["date"]
        for day in selected
        if (metric_value(day, "wind_gust_max_kmh") is not None and metric_value(day, "wind_gust_max_kmh") >= 50)
    ]
    wet_snow_dates = [
        day["date"]
        for day in selected
        if (
            metric_value(day, "snowfall_cm") is not None
            and metric_value(day, "snowfall_cm") > 0.1
            and metric_value(day, "temperature_min_c") is not None
            and metric_value(day, "temperature_min_c") <= 2
        )
    ]
    rain_snow_dates = [
        day["date"]
        for day in selected
        if (
            metric_value(day, "precipitation_mm") is not None
            and metric_value(day, "precipitation_mm") >= 1
            and metric_value(day, "snowfall_cm") is not None
            and metric_value(day, "snowfall_cm") > 0.1
        )
    ]
    freeze_dates = [
        day["date"]
        for day in selected
        if metric_value(day, "night_min_c") is not None and metric_value(day, "night_min_c") < 0
    ]
    weighted_after_september_20 = as_of_date >= dt.date(as_of_date.year, 9, 20)
    score = (
        (2 * len(strong_wind_dates) if weighted_after_september_20 else 0)
        + 2 * len(wet_snow_dates)
        + len(rain_snow_dates)
        + len(freeze_dates)
    )
    risk = "HIGH" if score >= 4 else "MEDIUM" if score >= 2 else "LOW"
    drivers = []
    if strong_wind_dates:
        drivers.append("gust")
    if wet_snow_dates:
        drivers.append("wet_snow")
    if rain_snow_dates:
        drivers.append("rain_snow")
    if freeze_dates:
        drivers.append("freeze")
    return {
        "weather_event_risk": risk,
        "drivers": drivers,
        "score": score,
        "seasonal_weighting_applied": weighted_after_september_20,
        "events": {
            "strong_wind_gust_ge_50_kmh_dates": strong_wind_dates,
            "wet_snow_dates": wet_snow_dates,
            "rain_snow_dates": rain_snow_dates,
            "freeze_night_dates": freeze_dates,
        },
        "interpretation": "weather event risk only; it does not determine whether leaves fall",
    }


def forecast_8_15d(days: list[dict]) -> dict:
    selected = days[7:15]
    means = [metric_value(day, "temperature_mean_c") for day in selected]
    means = [value for value in means if value is not None]
    precipitation = [metric_value(day, "precipitation_mm") for day in selected]
    gusts = [metric_value(day, "wind_gust_max_kmh") for day in selected]
    snowfall = [metric_value(day, "snowfall_cm") for day in selected]
    precision = {day.get("precision_class") for day in selected}
    if len(means) >= 2:
        change = means[-1] - means[0]
        temperature_trend = "warming" if change >= 1 else "cooling" if change <= -1 else "flat"
    else:
        change = None
        temperature_trend = "undetermined"
    precipitation = [value for value in precipitation if value is not None]
    gusts = [value for value in gusts if value is not None]
    snowfall = [value for value in snowfall if value is not None]
    return {
        "window_days": len(selected),
        "temperature_trend": temperature_trend,
        "temperature_change_first_to_last_c": round(change, 3) if change is not None else None,
        "moisture_trend": {
            "precipitation_total_mm": round(sum(precipitation), 3) if precipitation else None,
            "snowfall_total_cm": round(sum(snowfall), 3) if snowfall else None,
        },
        "wind_snow_trend": {
            "days_gust_ge_50_kmh": sum(value >= 50 for value in gusts),
            "days_with_snowfall": sum(value > 0.1 for value in snowfall),
        },
        "confidence": "LOW" if "trend_only_6h_plus" in precision or "mixed" in precision else "MEDIUM" if selected else "UNDETERMINED",
        "daily": selected,
    }


def agreement_label(condition: bool | None) -> str:
    if condition is None:
        return "UNDETERMINED"
    return "AGREE" if condition else "DISAGREE"


def gfs_crosscheck(hres_record: dict | None, gfs_record: dict | None) -> dict:
    if not hres_record or not gfs_record or hres_record.get("status") != "PASS" or gfs_record.get("status") != "PASS":
        return {
            "temperature_trend_agreement": "UNDETERMINED",
            "cold_window_agreement": "UNDETERMINED",
            "precipitation_agreement": "UNDETERMINED",
            "strong_wind_agreement": "UNDETERMINED",
            "reason": "HRES_OR_GFS_INVALID",
        }
    hres_days = {day["date"]: day for day in first_days(hres_record, 0, 7)}
    gfs_days = {day["date"]: day for day in first_days(gfs_record, 0, 7)}
    common_dates = sorted(set(hres_days) & set(gfs_days))
    if not common_dates:
        return {"temperature_trend_agreement": "UNDETERMINED", "cold_window_agreement": "UNDETERMINED", "precipitation_agreement": "UNDETERMINED", "strong_wind_agreement": "UNDETERMINED", "reason": "NO_COMMON_DATES"}
    temp_diffs = [
        abs(metric_value(hres_days[date], "temperature_mean_c") - metric_value(gfs_days[date], "temperature_mean_c"))
        for date in common_dates
        if metric_value(hres_days[date], "temperature_mean_c") is not None and metric_value(gfs_days[date], "temperature_mean_c") is not None
    ]
    hres_cold = {
        date
        for date in common_dates
        if metric_value(hres_days[date], "night_min_c") is not None and metric_value(hres_days[date], "night_min_c") < 5
    }
    gfs_cold = {
        date
        for date in common_dates
        if metric_value(gfs_days[date], "night_min_c") is not None and metric_value(gfs_days[date], "night_min_c") < 5
    }
    union = hres_cold | gfs_cold
    jaccard = len(hres_cold & gfs_cold) / len(union) if union else 1.0
    hres_precip = sum((metric_value(hres_days[date], "precipitation_mm") or 0) >= 1 for date in common_dates)
    gfs_precip = sum((metric_value(gfs_days[date], "precipitation_mm") or 0) >= 1 for date in common_dates)
    hres_wind = sum((metric_value(hres_days[date], "wind_gust_max_kmh") or 0) >= 50 for date in common_dates)
    gfs_wind = sum((metric_value(gfs_days[date], "wind_gust_max_kmh") or 0) >= 50 for date in common_dates)
    return {
        "common_dates": common_dates,
        "temperature_trend_agreement": agreement_label(max(temp_diffs) <= 2 if temp_diffs else None),
        "temperature_mean_absolute_difference_c": round(mean(temp_diffs), 3) if temp_diffs else None,
        "cold_window_agreement": agreement_label(jaccard >= 0.5),
        "cold_window_jaccard": round(jaccard, 3),
        "precipitation_agreement": agreement_label(abs(hres_precip - gfs_precip) <= 1),
        "precipitation_days": {"hres": hres_precip, "gfs": gfs_precip},
        "strong_wind_agreement": agreement_label(abs(hres_wind - gfs_wind) <= 1),
        "strong_wind_days": {"hres": hres_wind, "gfs": gfs_wind},
        "interpretation": "cross-check only; HRES and GFS are not averaged",
    }


def unavailable_long_range_summary(reason: str) -> dict:
    return {
        "status": "UNAVAILABLE",
        "reason": reason,
    }


def long_range_summary_for_chatgpt(region: dict | None) -> dict:
    """Expose only coarse 16-35 day labels in summary.json."""
    if not region:
        return unavailable_long_range_summary("LONG_RANGE_MODULE_UNAVAILABLE")
    region_status = region.get("status")
    if region_status == "UNAVAILABLE":
        return unavailable_long_range_summary(region.get("reason", "NO_VERIFIED_CORE_POINT"))
    if region_status == "FAILED":
        return {"status": "FAILED", "reason": region.get("reason", "LONG_RANGE_FETCH_FAILED")}
    overall = region.get("overall_16_35d") or {}
    notable_windows = []
    for window in overall.get("notable_windows", []):
        for signal in window.get("signals", []):
            notable_windows.append({
                "start_date": window.get("start_date"),
                "end_date": window.get("end_date"),
                "signal": signal,
            })
    evolution = [
        {
            "horizon_class": window.get("horizon_class"),
            "start_date": window.get("start_date"),
            "end_date": window.get("end_date"),
            "status": (window.get("signal_evolution") or {}).get("status", "INSUFFICIENT_HISTORY"),
            "runs_seen": (window.get("signal_evolution") or {}).get("runs_seen", 0),
            "trend": (window.get("signal_evolution") or {}).get("trend", "UNDETERMINED"),
        }
        for window in region.get("windows", [])
    ]
    return {
        "status": "PASS" if region_status == "OK" else "PARTIAL",
        "temperature_background": (overall.get("temperature") or {}).get("direction", "UNDETERMINED"),
        "cold_air_signal": (overall.get("cold_air") or {}).get("signal", "UNDETERMINED"),
        "precipitation_signal": (overall.get("moisture") or {}).get("signal", "UNDETERMINED"),
        "snow_signal": (overall.get("snow") or {}).get("signal", "UNDETERMINED"),
        "strong_wind_signal": (overall.get("wind") or {}).get("signal", "UNDETERMINED"),
        "uncertainty": overall.get("uncertainty", "VERY_HIGH"),
        "notable_windows": notable_windows,
        "signal_evolution": evolution,
        "interpretation": "16-35 day background signal only; not a date-level forecast or phenology lead/lag calculation",
    }


def flattened_lightweight_window(window: dict | None) -> dict:
    """Flatten the small window contract; never carry daily/hourly arrays here."""
    if not isinstance(window, dict):
        return {"status": "UNAVAILABLE", "metrics": None, "reason": "WINDOW_UNAVAILABLE"}
    output = {
        key: copy.deepcopy(window.get(key))
        for key in (
            "status",
            "start_date",
            "end_date",
            "expected_days",
            "days_available",
            "missing_dates",
            "incomplete_dates",
            "grid_count",
            "reason",
        )
        if key in window
    }
    metrics = window.get("metrics")
    if isinstance(metrics, dict):
        output.update({key: copy.deepcopy(metrics.get(key)) for key in LIGHTWEIGHT_WINDOW_METRIC_KEYS})
        if "temperature_trend" in metrics:
            output["temperature_trend"] = copy.deepcopy(metrics["temperature_trend"])
        if "solar_variable" in metrics:
            output["solar_variable"] = metrics["solar_variable"]
        if "solar_unit" in metrics:
            output["solar_unit"] = metrics["solar_unit"]
    else:
        output["metrics"] = None
    if output.get("status") == HISTORY_FORWARD_WINDOW_NOT_APPLICABLE:
        # The lightweight views consumed by the phenology summary and the compact
        # region paths only speak OK/PARTIAL/INVALID/UNAVAILABLE.  A structurally
        # closed rolling window carries no data for those readers, which is exactly
        # UNAVAILABLE; the original reason stays so callers can still tell a closed
        # window apart from a failed fetch.
        output["status"] = "UNAVAILABLE"
    return output


def _month_day_label(value: str | None) -> str | None:
    if not value:
        return None
    parsed = dt.date.fromisoformat(value)
    return f"{parsed.month}/{parsed.day}"


def _forecast_window_availability_note(window: dict, status: str) -> str | None:
    if status == "OK":
        return None
    if status == "INVALID":
        return "窗口无有效数据，不进入主链"
    if status != "PARTIAL":
        return "窗口不可用，不进入主链"
    pending = sorted({
        value
        for key in ("missing_dates", "incomplete_dates")
        for value in (window.get(key) or [])
        if value
    })
    start = window.get("start_date")
    end = window.get("end_date")
    if not pending or not start or not end:
        return "窗口部分可用，可作趋势参考"
    start_date = dt.date.fromisoformat(start)
    end_date = dt.date.fromisoformat(end)
    first_pending = dt.date.fromisoformat(pending[0])
    expected_pending = []
    cursor = first_pending
    while cursor <= end_date:
        expected_pending.append(cursor.isoformat())
        cursor += dt.timedelta(days=1)
    if first_pending > start_date and pending == expected_pending:
        available_end = first_pending - dt.timedelta(days=1)
        available_range = (
            f"{start_date.month}/{start_date.day}–{available_end.day}"
            if start_date.month == available_end.month
            else f"{_month_day_label(start)}–{_month_day_label(available_end.isoformat())}"
        )
        return (
            f"{available_range}预测，"
            f"{_month_day_label(first_pending.isoformat())}待补"
        )
    pending_label = "、".join(_month_day_label(value) or value for value in pending)
    return f"窗口部分可用，可作趋势参考；待补：{pending_label}"


def forecast_window_view(window: dict | None) -> dict:
    """Expose forecast availability per window instead of gating the region globally."""
    output = flattened_lightweight_window(window)
    status = output.get("status", "UNAVAILABLE")
    raw_window = window if isinstance(window, dict) else {}
    has_metrics = isinstance(raw_window.get("metrics"), dict)
    output["usable_for_main_chain"] = status == "OK"
    output["usable_for_trend_reference"] = status in {"OK", "PARTIAL"} and bool(
        output.get("days_available", 0) and has_metrics
    )
    output["availability_note"] = _forecast_window_availability_note(output, status)
    return output


def unavailable_lightweight_windows(forecast_date: dt.date, reason: str) -> dict:
    return {
        key: flattened_lightweight_window(
            lightweight_window_summary([], definition, allow_partial=True)
            | {"status": "UNAVAILABLE", "reason": reason}
        )
        for key, definition in history_forward_windows_for_year(forecast_date, 2026).items()
    }


def unavailable_forecast_windows(forecast_date: dt.date, reason: str) -> dict:
    return {
        key: forecast_window_view(
            lightweight_window_summary([], definition, allow_partial=True)
            | {"status": "UNAVAILABLE", "reason": reason}
        )
        for key, definition in history_forward_windows_for_year(forecast_date, 2026).items()
    }


def light_sampling_from_history_subregion(subregion: dict) -> dict:
    by_year = subregion.get("sampling", {}).get("by_year", {})
    first = next(iter(by_year.values()), {})
    return {
        "status": subregion.get("status", "INVALID"),
        "requested_points": first.get("requested_points", len(subregion.get("point_ids", []))),
        "verified_points": first.get("verified_points", len(subregion.get("verified_point_ids", []))),
        "unique_model_grids": first.get("unique_model_grids", 0),
        "grid_coordinates": copy.deepcopy(first.get("grid_coordinates", [])),
        "grid_cell_ids": copy.deepcopy(first.get("grid_cell_ids", [])),
        "same_unique_grid_set_across_years": subregion.get("sampling", {}).get("same_unique_grid_set_across_years", False),
        "by_year": {
            year: {
                "status": item.get("status", "INVALID"),
                "unique_model_grids": item.get("unique_model_grids", 0),
                "grid_coordinates": copy.deepcopy(item.get("grid_coordinates", [])),
            }
            for year, item in by_year.items()
        },
        "reason": subregion.get("reason"),
    }


def light_sampling_from_records(
    config: dict,
    point_ids: list[str],
    records: dict[str, dict],
    *,
    minimum_verified_unique_grids: int = 1,
) -> dict:
    sampling = grid_sampling_summary(
        config,
        point_ids,
        records,
        minimum_verified_unique_grids=minimum_verified_unique_grids,
    )
    return {
        key: copy.deepcopy(sampling[key])
        for key in (
            "status",
            "requested_points",
            "verified_points",
            "queried_points",
            "valid_points",
            "failed_points",
            "excluded_point_ids",
            "valid_point_ids",
            "unique_model_grids",
            "grid_coordinates",
            "grid_cell_ids",
            "minimum_verified_unique_grids",
            "reason",
            "deduplication",
        )
        if key in sampling
    }


def point_year_lightweight_views(
    config: dict,
    point_id: str,
    hres: dict,
    history_forward: dict,
    forecast_date: dt.date,
) -> tuple[dict, dict]:
    """Build the small four-year view for a single non-Siguniang core point."""
    definitions_2026 = history_forward_windows_for_year(forecast_date, 2026)
    years = {}
    forward_point = (history_forward.get("points") or {}).get(point_id) or {}
    for year in HISTORY_FORWARD_YEARS:
        record = (forward_point.get("years") or {}).get(str(year))
        if record and record.get("status") == "PASS":
            definitions = history_forward_windows_for_year(forecast_date, year)
            years[str(year)] = {
                key: flattened_lightweight_window(
                    lightweight_window_summary(record.get("daily", []), definition, allow_partial=False)
                )
                for key, definition in definitions.items()
            }
        else:
            years[str(year)] = unavailable_lightweight_windows(forecast_date, "HISTORY_FORWARD_POINT_INVALID")
    forecast_record = (hres.get("points") or {}).get(point_id)
    if forecast_record and forecast_record.get("status") == "PASS":
        years["2026"] = {
            key: forecast_window_view(
                lightweight_window_summary(forecast_record.get("daily", []), definition, allow_partial=True)
            )
            for key, definition in definitions_2026.items()
        }
    else:
        years["2026"] = unavailable_forecast_windows(forecast_date, "HRES_POINT_INVALID")
    history_sampling = {}
    for year in HISTORY_FORWARD_YEARS:
        record = (forward_point.get("years") or {}).get(str(year))
        history_sampling[str(year)] = light_sampling_from_records(
            config,
            [point_id],
            {point_id: record} if record else {},
            minimum_verified_unique_grids=1,
        )
    forecast_sampling = light_sampling_from_records(
        config,
        [point_id],
        {point_id: forecast_record} if forecast_record else {},
        minimum_verified_unique_grids=1,
    )
    sampling_status = "OK" if forecast_sampling.get("status") == "OK" and all(
        item.get("status") == "OK" for item in history_sampling.values()
    ) else "PARTIAL"
    sampling = {
        "status": sampling_status,
        "forecast_2026": forecast_sampling,
        "historical_2023_2025": {
            "status": "OK" if all(item.get("status") == "OK" for item in history_sampling.values()) else "FAILED",
            "by_year": history_sampling,
        },
    }
    return years, sampling


def build_region_forecast_subregion(
    config: dict,
    region_id: str,
    subregion_id: str,
    hres: dict,
    forecast_date: dt.date,
) -> dict:
    registry_item = region_subregion_registry(config, region_id).get(subregion_id) or {}
    point_ids = region_subregion_point_ids(config, region_id, subregion_id)
    records = {
        point_id: (hres.get("points") or {}).get(point_id)
        for point_id in region_subregion_point_ids(config, region_id, subregion_id, verified_only=True)
        if (hres.get("points") or {}).get(point_id)
    }
    minimum_grids = registry_item.get("minimum_verified_unique_grids", 1)
    sampling = grid_sampling_summary(
        config,
        point_ids,
        records,
        minimum_verified_unique_grids=minimum_grids,
    )
    unique_records = [entry["record"] for entry in deduplicate_grid_records(list(records.values()))]
    years = {"2026": {"status": "OK", "sampling": light_sampling_from_records(
        config,
        point_ids,
        records,
        minimum_verified_unique_grids=minimum_grids,
    )}}
    definitions = history_forward_windows_for_year(forecast_date, 2026)
    for key, definition in definitions.items():
        years["2026"][key] = aggregate_grid_window(unique_records, definition, allow_partial=True)
        if years["2026"][key].get("status") == "INVALID":
            years["2026"]["status"] = "INVALID"
    if not unique_records:
        status = "INVALID"
    elif sampling.get("status") == "OK" and years["2026"]["status"] == "OK":
        status = "OK"
    else:
        status = "PARTIAL"
    return {
        "subregion": subregion_id,
        "name": registry_item.get("name", subregion_id),
        "status": status,
        "usable_for_main_chain": bool(unique_records)
        and years["2026"].get("d0_7", {}).get("status") == "OK",
        "sampling": light_sampling_from_records(
            config,
            point_ids,
            records,
            minimum_verified_unique_grids=minimum_grids,
        ),
        "years": years,
        "reason": None if status == "OK" else sampling.get("reason") or "HRES_SUBREGION_PARTIAL",
    }


def build_siguniang_forecast_subregion(
    config: dict,
    subregion_id: str,
    hres: dict,
    forecast_date: dt.date,
) -> dict:
    return build_region_forecast_subregion(config, "siguniang", subregion_id, hres, forecast_date)


def build_jiuzhaigou_forecast_subregion(
    config: dict,
    subregion_id: str,
    hres: dict,
    forecast_date: dt.date,
) -> dict:
    return build_region_forecast_subregion(config, "jiuzhaigou", subregion_id, hres, forecast_date)


def combine_region_light_years(
    subregions: dict[str, dict],
    history_composite: dict | None,
    forecast_date: dt.date,
    subregion_keys: tuple[str, ...],
    *,
    region_id: str,
) -> dict:
    years = {}
    for year in HISTORY_FORWARD_YEARS:
        source_year = (history_composite or {}).get("years", {}).get(str(year), {})
        definitions = history_forward_windows_for_year(forecast_date, year)
        years[str(year)] = {
            key: flattened_lightweight_window(source_year.get(key))
            if source_year.get(key)
            else flattened_lightweight_window(
                lightweight_window_summary([], definition, allow_partial=True)
                | {"status": "UNAVAILABLE", "reason": "HISTORY_FORWARD_COMPOSITE_UNAVAILABLE"}
            )
            for key, definition in definitions.items()
        }
    definitions_2026 = history_forward_windows_for_year(forecast_date, 2026)
    forecast_year = {"status": "OK"}
    for key, definition in definitions_2026.items():
        items = []
        for subregion_id in subregion_keys:
            item = subregions.get(subregion_id) or {}
            window = (item.get("years", {}).get("2026", {}) or {}).get(key)
            if not window:
                items.append(
                    lightweight_window_summary([], definition, allow_partial=True)
                    | {"reason": "SUBREGION_WINDOW_UNAVAILABLE"}
                )
            else:
                items.append(window)
        forecast_year[key] = equal_mean_subregion_window(items, definition, region_id=region_id)
        if forecast_year[key].get("status") != "OK":
            forecast_year["status"] = "PARTIAL"
    years["2026"] = {
        key: forecast_window_view(forecast_year.get(key))
        for key in definitions_2026
    }
    return years


def combine_siguniang_light_years(
    subregions: dict[str, dict],
    history_composite: dict | None,
    forecast_date: dt.date,
) -> dict:
    return combine_region_light_years(
        subregions,
        history_composite,
        forecast_date,
        SIGUNIANG_SUBREGION_KEYS,
        region_id="siguniang",
    )


def combine_jiuzhaigou_light_years(
    subregions: dict[str, dict],
    history_composite: dict | None,
    forecast_date: dt.date,
) -> dict:
    return combine_region_light_years(
        subregions,
        history_composite,
        forecast_date,
        JIUZHAIGOU_SUBREGION_KEYS,
        region_id="jiuzhaigou",
    )


def build_grid_registry(config: dict, hres: dict, history_forward: dict, generated_at: str, data_date: str) -> dict:
    """Persist the point-to-grid mapping without duplicating weather time series."""
    hres_points = hres.get("points") or {}
    forward_points = history_forward.get("points") or {}
    hres_mappings = {
        point_id: point_grid_mapping(record, point_id=point_id)
        for point_id, record in hres_points.items()
    }
    history_mappings = {}
    for point_id, result in forward_points.items():
        history_mappings[point_id] = {
            "point_id": point_id,
            "years": {
                year: point_grid_mapping(record, point_id=point_id, year=int(year))
                for year, record in (result.get("years") or {}).items()
            },
            "same_grid_qa": copy.deepcopy(result.get("same_grid_qa")),
        }
    subregions_by_region = {}
    for region_id, subregion_keys in SUBREGION_KEYS_BY_REGION.items():
        subregions = {}
        for subregion_id in subregion_keys:
            point_ids = region_subregion_point_ids(config, region_id, subregion_id)
            subregions[subregion_id] = {
                "point_ids": point_ids,
                "verified_point_ids": region_subregion_point_ids(
                    config,
                    region_id,
                    subregion_id,
                    verified_only=True,
                ),
                "hres_unique_grid_ids": [
                    entry["grid_cell_id"]
                    for entry in deduplicate_grid_records(
                        [hres_points[point_id] for point_id in point_ids if point_id in hres_points]
                    )
                ],
                "history_unique_grid_ids_by_year": {
                    year: [
                        entry["grid_cell_id"]
                        for entry in deduplicate_grid_records(
                            [
                                (forward_points.get(point_id, {}).get("years") or {}).get(year)
                                for point_id in point_ids
                                if (forward_points.get(point_id, {}).get("years") or {}).get(year)
                            ]
                        )
                    ]
                    for year in (str(value) for value in HISTORY_FORWARD_YEARS)
                },
            }
        if subregions:
            subregions_by_region[region_id] = subregions
    return {
        "schema_version": SCHEMA_VERSION,
        "module": "grid_registry",
        "generated_at": generated_at,
        "data_date": data_date,
        "source": "Open-Meteo",
        "deduplication_key": "returned_grid_coordinate",
        "hres": {"points": hres_mappings},
        "history_forward": {"points": history_mappings},
        "siguniang_subregions": subregions_by_region.get("siguniang", {}),
        "jiuzhaigou_subregions": subregions_by_region.get("jiuzhaigou", {}),
        "subregions_by_region": subregions_by_region,
        "interpretation_boundary": "Mapping and QA metadata only; no weather series or downstream ecological conclusion.",
    }


def build_registered_light_region(
    config: dict,
    region_id: str,
    subregion_keys: tuple[str, ...],
    generated_at: str,
    data_date: str,
    forecast_date: dt.date,
    hres: dict,
    history_forward: dict,
) -> dict:
    region_config = config.get("regions", {}).get(region_id, {})
    history_region = (history_forward.get("regions") or {}).get(region_id) or {}
    subregion_views = {}
    for subregion_id in subregion_keys:
        history_subregion = (history_region.get("subregions") or {}).get(subregion_id)
        forecast_subregion = build_region_forecast_subregion(
            config,
            region_id,
            subregion_id,
            hres,
            forecast_date,
        )
        if history_subregion:
            history_years = {
                str(year): {
                    key: flattened_lightweight_window(
                        (history_subregion.get("years") or {}).get(str(year), {}).get(key)
                    )
                    for key in HISTORY_FORWARD_WINDOW_KEYS
                }
                for year in HISTORY_FORWARD_YEARS
            }
        else:
            history_years = {
                str(year): unavailable_lightweight_windows(
                    forecast_date,
                    "HISTORY_FORWARD_SUBREGION_UNAVAILABLE",
                )
                for year in HISTORY_FORWARD_YEARS
            }
        forecast_year = {
            key: forecast_window_view(
                (forecast_subregion.get("years") or {}).get("2026", {}).get(key)
            )
            for key in HISTORY_FORWARD_WINDOW_KEYS
        }
        forecast_status = forecast_subregion.get("status")
        history_status = (history_subregion or {}).get("status")
        forecast_sampling = forecast_subregion.get("sampling") or {}
        historical_sampling = (
            light_sampling_from_history_subregion(history_subregion)
            if history_subregion
            else {"status": "INVALID", "reason": "HISTORY_FORWARD_SUBREGION_UNAVAILABLE"}
        )
        if forecast_status == "OK" and history_status == "OK":
            subregion_status = "OK"
        elif forecast_subregion.get("usable_for_main_chain") or (history_subregion or {}).get("usable_for_main_chain"):
            subregion_status = "PARTIAL"
        else:
            subregion_status = "INVALID"
        if forecast_sampling.get("status") == "OK" and historical_sampling.get("status") == "OK":
            sampling_status = "OK"
        elif forecast_sampling.get("status") in {"OK", "PARTIAL"} or historical_sampling.get("status") in {"OK", "PARTIAL"}:
            sampling_status = "PARTIAL"
        else:
            sampling_status = "INVALID"
        subregion_views[subregion_id] = {
            "name": forecast_subregion.get("name") or (history_subregion or {}).get("name"),
            "status": subregion_status,
            "usable_for_main_chain": forecast_year["d0_7"].get("usable_for_main_chain", False),
            "sampling": {
                "status": sampling_status,
                "forecast_2026": forecast_sampling,
                "historical_2023_2025": historical_sampling,
            },
            "years": {**history_years, "2026": forecast_year},
            "reason": (
                forecast_subregion.get("reason")
                if forecast_status != "OK"
                else (history_subregion or {}).get("reason")
            ),
        }
    history_composite = history_region.get("composite")
    composite_years = combine_region_light_years(
        subregion_views,
        history_composite,
        forecast_date,
        subregion_keys,
        region_id=region_id,
    )
    statuses = [item.get("status") for item in subregion_views.values()]
    if statuses and all(status == "OK" for status in statuses):
        composite_status = "OK"
    elif any(status in {"OK", "PARTIAL"} for status in statuses):
        composite_status = "PARTIAL"
    else:
        composite_status = "INVALID"
    composite_forecast = composite_years.get("2026", {})
    composite_usable_for_main_chain = composite_forecast.get("d0_7", {}).get(
        "usable_for_main_chain",
        False,
    )
    composite_sampling_status = (
        "OK"
        if subregion_views and all(
            (item.get("sampling") or {}).get("status") == "OK"
            for item in subregion_views.values()
        )
        else "PARTIAL"
        if any(
            (item.get("sampling") or {}).get("status") in {"OK", "PARTIAL"}
            for item in subregion_views.values()
        )
        else "INVALID"
    )
    return {
        "name": region_config.get("name", region_id),
        "usable_for_main_chain": composite_usable_for_main_chain,
        "status": composite_status,
        "subregions": subregion_views,
        "composite": {
            "status": composite_status,
            "usable_for_main_chain": composite_usable_for_main_chain,
            "aggregation": (
                f"equal_mean_of_{region_id}_subregions; "
                f"{', '.join(subregion_keys)} each weight=1/{len(subregion_keys)}; "
                "no point-count weighting"
            ),
            "sampling": {
                "status": composite_sampling_status,
                "subregions": {
                    key: copy.deepcopy(value.get("sampling"))
                    for key, value in subregion_views.items()
                },
            },
            "years": composite_years,
            "missing_or_partial_subregions": [
                key for key, item in subregion_views.items() if item.get("status") != "OK"
            ],
            "reason": (
                None
                if composite_status == "OK"
                else f"{region_id.upper()}_COMPOSITE_REQUIRES_ALL_SUBREGIONS"
            ),
        },
    }


def weather_event_light_summary(weather_events: dict | None, region_id: str) -> dict:
    """Return only the small next-0-7-day event view for the compact artifact."""
    if not weather_events:
        return {"status": "UNAVAILABLE", "reason": "WEATHER_EVENTS_MODULE_UNAVAILABLE"}
    region = (weather_events.get("regions") or {}).get(region_id) or {}
    forecast = region.get("forecast") or {}
    window = (forecast.get("windows") or {}).get("d0_7") or {}
    metrics = window.get("metrics") or {}
    if not metrics:
        return {
            "status": "UNAVAILABLE",
            "reason": "WEATHER_EVENTS_FORECAST_WINDOW_UNAVAILABLE",
        }
    episodes = ((region.get("cooling_episode_candidates") or {}).get("forecast") or [])
    return {
        "status": window.get("status", "UNAVAILABLE"),
        "next_0_7d_max_gust_kmh": metrics.get("wind_gust_max_kmh"),
        "next_0_7d_precip_total_mm": metrics.get("precipitation_total_mm"),
        "next_0_7d_snowfall_total_cm": metrics.get("snowfall_total_cm"),
        "next_0_7d_gust_ge_50_days": metrics.get("gust_ge_50_days"),
        "next_0_7d_combined_weather_stress_events": metrics.get("combined_weather_stress_events"),
        "mechanical_leaf_stress": copy.deepcopy(
            metrics.get("mechanical_leaf_stress") or {
                "level": "UNDETERMINED",
                "reasons": [],
                "rule_version": MECHANICAL_LEAF_STRESS_RULE_VERSION,
            }
        ),
        "current_cooling_episode_candidate": copy.deepcopy(episodes[0]) if episodes else None,
        "interpretation_boundary": "Weather mechanical pressure only; no ecological or travel conclusion.",
    }


def _forecast_hour_indices(record: dict, target_date: dt.date, window: str, cutoff_date: dt.date) -> list[int]:
    hourly = record.get("hourly") or {}
    times = hourly.get("time") if isinstance(hourly.get("time"), list) else []
    return _gefs_window_indices(times, target_date, window, cutoff_date)


def _deterministic_hourly_window(record: dict | None, target_date: dt.date, window: str, cutoff_date: dt.date) -> dict:
    if not record or record.get("status") != "PASS":
        return {"status": "UNAVAILABLE", "reason": "DETERMINISTIC_MODULE_UNAVAILABLE"}
    hourly = record.get("hourly") or {}
    indices = _forecast_hour_indices(record, target_date, window, cutoff_date)
    if not indices:
        return {"status": "UNAVAILABLE", "reason": "WINDOW_OUTSIDE_FORECAST_HORIZON"}

    def values(variable: str) -> list[float]:
        return _values_for_indices(hourly, variable, indices)

    temperatures = values("temperature_2m")
    dew_point = values("dew_point_2m")
    humidity = values("relative_humidity_2m")
    clouds = values("cloud_cover")
    low_clouds = values("cloud_cover_low")
    mid_clouds = values("cloud_cover_mid")
    high_clouds = values("cloud_cover_high")
    precipitation = values("precipitation")
    rain = values("rain")
    snowfall = values("snowfall")
    gusts = values("wind_gusts_10m")
    wind = values("wind_speed_10m")
    wind_direction = values("wind_direction_10m")
    solar_variable = record.get("solar_variable")
    solar = values(solar_variable) if solar_variable else []
    result = {
        "status": "OK",
        "window": window,
        "date": target_date.isoformat(),
        "hours_included": len(indices),
        "total_cloud_pct": round(mean(clouds), 3) if clouds else None,
        "low_cloud_pct": round(mean(low_clouds), 3) if low_clouds else None,
        "mid_cloud_pct": round(mean(mid_clouds), 3) if mid_clouds else None,
        "high_cloud_pct": round(mean(high_clouds), 3) if high_clouds else None,
        "precipitation_mm": round(sum(precipitation), 3) if precipitation else None,
        "rain_mm": round(sum(rain), 3) if rain else None,
        "snowfall_cm": round(sum(snowfall), 3) if snowfall else None,
        "temperature_mean_c": round(mean(temperatures), 3) if temperatures else None,
        "temperature_min_c": round(min(temperatures), 3) if temperatures else None,
        "temperature_max_c": round(max(temperatures), 3) if temperatures else None,
        "dew_point_mean_c": round(mean(dew_point), 3) if dew_point else None,
        "relative_humidity_mean_pct": round(mean(humidity), 3) if humidity else None,
        "gust_max_kmh": round(max(gusts), 3) if gusts else None,
        "wind_speed_mean_kmh": round(mean(wind), 3) if wind else None,
        "wind_direction_mean_deg": circular_mean_degrees(wind_direction),
        "wind_direction_resultant_length": circular_resultant_length(wind_direction),
        "sunshine_or_shortwave": (
            {"variable": solar_variable, "value": round(sum(solar), 3)}
            if solar and solar_variable == "sunshine_duration"
            else {"variable": solar_variable, "value": round(mean(solar), 3)}
            if solar
            else None
        ),
    }
    if window == "NIGHT" and target_date == cutoff_date:
        result["cutoff_truncated"] = True
    return result


def _deterministic_daily_summary(record: dict | None, target_date: dt.date, cutoff_date: dt.date) -> dict:
    if not record or record.get("status") != "PASS":
        return {"status": "UNAVAILABLE", "reason": "DETERMINISTIC_MODULE_UNAVAILABLE"}
    date_key = target_date.isoformat()
    item = next((day for day in record.get("daily", []) if day.get("date") == date_key), None)
    if not item or target_date > cutoff_date:
        return {"status": "UNAVAILABLE", "reason": "DATE_OUTSIDE_FORECAST_HORIZON"}
    return {
        "status": "OK" if item.get("complete") else "PARTIAL",
        "date": date_key,
        "total_cloud_pct": item.get("cloud_cover_mean_pct"),
        "low_cloud_pct": item.get("cloud_cover_low_mean_pct"),
        "mid_cloud_pct": item.get("cloud_cover_mid_mean_pct"),
        "high_cloud_pct": item.get("cloud_cover_high_mean_pct"),
        "precipitation_mm": item.get("precipitation_mm"),
        "rain_mm": item.get("rain_mm"),
        "snowfall_cm": item.get("snowfall_cm"),
        "temperature_mean_c": item.get("temperature_mean_c"),
        "temperature_min_c": item.get("temperature_min_c"),
        "temperature_max_c": item.get("temperature_max_c"),
        "dew_point_mean_c": item.get("dew_point_mean_c"),
        "relative_humidity_mean_pct": item.get("relative_humidity_mean_pct"),
        "gust_max_kmh": item.get("wind_gust_max_kmh"),
        "gust_mean_kmh": item.get("wind_gust_mean_kmh"),
        "wind_speed_mean_kmh": item.get("wind_speed_mean_kmh"),
        "wind_direction_mean_deg": item.get("wind_direction_mean_deg"),
        "wind_direction_resultant_length": item.get("wind_direction_member_resultant_length"),
        "sunshine_or_shortwave": item.get("solar_metric"),
    }


def _ensemble_window_view(record: dict | None, target_date: dt.date, window: str, cutoff_date: dt.date) -> dict:
    if not record or record.get("status") != "PASS":
        return {"status": "UNAVAILABLE", "reason": "ECMWF_ENSEMBLE_UNAVAILABLE"}
    hourly = record.get("hourly") or {}
    times = hourly.get("time") if isinstance(hourly.get("time"), list) else []
    indices = _gefs_window_indices(times, target_date, window, cutoff_date)
    if not indices:
        return {"status": "UNAVAILABLE", "reason": "OUTSIDE_ECMWF_ENSEMBLE_HORIZON"}
    valid, check = _ec_ensemble_member_check({"time": times, **hourly})
    suffixes = check.get("member_suffixes", []) if valid else []
    solar_variable = record.get("solar_variable")
    if solar_variable not in UNIFIED_SOLAR_VARIABLES:
        solar_variable = "sunshine_duration" if "sunshine_duration" in hourly else None
    values = []
    for suffix in suffixes:
        item = _gefs_member_aggregate(hourly, suffix, indices, solar_variable)
        if item:
            values.append(item)
    if not values:
        return {"status": "UNAVAILABLE", "reason": "ECMWF_ENSEMBLE_WINDOW_MISSING"}
    result = _gefs_distribution_summary(values, len(suffixes))
    meta = record.get("ensemble") or {}
    return {
        "status": "OK" if check.get("status") == "PASS" else "PARTIAL",
        "window": window,
        "date": target_date.isoformat(),
        "model_id": meta.get("model_id", "ecmwf_ifs025_ensemble"),
        "resolution": meta.get("resolution"),
        "members_valid": len(suffixes),
        "expected_members": EC_ENSEMBLE_TOTAL_MEMBERS,
        "member_check_status": check.get("status"),
        "optional_unavailable_variables": check.get("optional_missing_variables", []),
        "required_unavailable_variables": check.get("required_missing_variables", []),
        "statistics": result,
    }


def _gefs_segment_for_date(point_record: dict | None, target_date: dt.date) -> tuple[str | None, dict | None]:
    if not point_record:
        return None, None
    for segment_key in ("near_range", "long_range"):
        segment = point_record.get(segment_key) or {}
        if any(day.get("date") == target_date.isoformat() for day in segment.get("daily", [])):
            return segment_key, segment
    return None, None


def _gefs_window_view(point_record: dict | None, target_date: dt.date, window: str) -> dict:
    segment_key, segment = _gefs_segment_for_date(point_record, target_date)
    if not segment:
        return {"status": "UNAVAILABLE", "reason": "GEFS_DATE_OUTSIDE_FORECAST_HORIZON"}
    result = copy.deepcopy((segment.get("windows") or {}).get(target_date.isoformat(), {}).get(window) or {})
    if not result:
        return {"status": "UNAVAILABLE", "reason": "GEFS_WINDOW_UNAVAILABLE"}
    result["segment"] = segment_key
    result["model_id"] = segment.get("model_id")
    result["resolution"] = segment.get("resolution")
    return result


def _distribution_classification(value: float | None, stats: dict | None) -> str:
    if value is None or not stats or stats.get("p10") is None:
        return "UNAVAILABLE"
    if stats.get("p25") is not None and stats.get("p75") is not None and stats["p25"] <= value <= stats["p75"]:
        return "INSIDE_IQR"
    if stats.get("p10") <= value <= stats.get("p90"):
        return "INSIDE_P10_P90"
    return "OUTLIER"


def _event_support_from_gefs_window(gefs_window: dict, event_type: str) -> float | None:
    statistics = gefs_window.get("statistics") or {}
    probabilities = statistics.get("probabilities") or {}
    mapping = {
        "CLOUD_EVENT": "cloud_cover_gt_70pct",
        "PRECIP_EVENT": "precipitation_gt_0_5mm",
        "SNOW_EVENT": "snowfall_gt_0_5cm",
    }
    item = probabilities.get(mapping.get(event_type, ""))
    return item.get("probability") if isinstance(item, dict) else None


def _deterministic_support(
    deterministic: dict,
    gefs_window: dict,
    event_type: str,
) -> str:
    if deterministic.get("status") != "OK" or gefs_window.get("status") not in {"OK", "PARTIAL"}:
        return "UNAVAILABLE"
    if event_type == "CLOUD_EVENT":
        active = (deterministic.get("total_cloud_pct") or 0) >= GEFS_PHASE_THRESHOLDS["cloud_cover_pct"] or (deterministic.get("low_cloud_pct") or 0) >= GEFS_PHASE_THRESHOLDS["cloud_cover_low_pct"]
    elif event_type == "PRECIP_EVENT":
        active = (deterministic.get("precipitation_mm") or 0) >= GEFS_PHASE_THRESHOLDS["precipitation_mm"]
    elif event_type == "SNOW_EVENT":
        active = (deterministic.get("snowfall_cm") or 0) > GEFS_PHASE_THRESHOLDS["snowfall_cm"]
    else:
        active = False
    support = _event_support_from_gefs_window(gefs_window, event_type)
    if support is None:
        return "UNAVAILABLE"
    if active and support >= 0.6:
        return "SUPPORTED"
    if not active and support < 0.3:
        return "SUPPORTED"
    if 0.2 <= support < 0.6:
        return "WEAK_SUPPORT"
    return "OUTLIER"


def _deterministic_consistency(gfs_window: dict, gefs_window: dict) -> dict:
    """Compare one deterministic window against one ensemble window.

    Generic over the model pair: the same routine answers
    HRES-vs-EC-ensemble, GFS-vs-GEFS and HRES-vs-GEFS.  A layer that the
    ensemble cannot provide stays UNAVAILABLE instead of being replaced by the
    total cloud cover.
    """
    if gfs_window.get("status") != "OK" or gefs_window.get("status") not in {"OK", "PARTIAL"}:
        return {
            "cloud_cover": "UNAVAILABLE",
            "low_cloud": "UNAVAILABLE",
            "mid_cloud": "UNAVAILABLE",
            "high_cloud": "UNAVAILABLE",
            "temperature": "UNAVAILABLE",
            "snowfall": "UNAVAILABLE",
            "precipitation": "UNAVAILABLE",
            "wind": "UNAVAILABLE",
            "gust": "UNAVAILABLE",
            "event_phase": "UNAVAILABLE",
            "deterministic_outlier": False,
        }
    stats = gefs_window.get("statistics") or {}
    classifications = {
        "cloud_cover": _distribution_classification(gfs_window.get("total_cloud_pct"), (stats.get("cloud_cover") or {})),
        "low_cloud": _distribution_classification(gfs_window.get("low_cloud_pct"), (stats.get("cloud_cover_low") or {})),
        "mid_cloud": _distribution_classification(gfs_window.get("mid_cloud_pct"), (stats.get("cloud_cover_mid") or {})),
        "high_cloud": _distribution_classification(gfs_window.get("high_cloud_pct"), (stats.get("cloud_cover_high") or {})),
        "temperature": _distribution_classification(gfs_window.get("temperature_mean_c"), (stats.get("temperature_2m") or {})),
        "snowfall": _distribution_classification(gfs_window.get("snowfall_cm"), (stats.get("snowfall") or {})),
        "precipitation": _distribution_classification(gfs_window.get("precipitation_mm"), (stats.get("precipitation") or {})),
        "wind": _distribution_classification(gfs_window.get("wind_speed_mean_kmh"), (stats.get("wind_speed_10m") or {})),
        # Gusts are compared against the gust distribution, never against the
        # sustained wind distribution.
        "gust": _distribution_classification(gfs_window.get("gust_max_kmh"), (stats.get("wind_gusts_10m") or {})),
    }
    supports = [_deterministic_support(gfs_window, gefs_window, event_type) for event_type in ("CLOUD_EVENT", "PRECIP_EVENT", "SNOW_EVENT")]
    outlier = any(value == "OUTLIER" for value in classifications.values()) or any(value == "OUTLIER" for value in supports)
    return {
        **classifications,
        "event_phase": "OUTLIER" if any(value == "OUTLIER" for value in supports) else "SUPPORTED" if all(value in {"SUPPORTED", "UNAVAILABLE"} for value in supports) else "WEAK_SUPPORT",
        "deterministic_outlier": outlier,
    }


def _ensemble_probability_consistency(
    first_window: dict,
    second_window: dict,
    *,
    pair_label: str,
) -> dict:
    """Probability-space agreement between two independent ensembles.

    The two ensembles are compared, never averaged.
    """
    first_available = first_window.get("status") in {"OK", "PARTIAL"}
    second_available = second_window.get("status") in {"OK", "PARTIAL"}
    if not first_available and not second_available:
        return {"agreement": "UNAVAILABLE", "pair": pair_label, "notes": ["BOTH_ENSEMBLES_UNAVAILABLE"]}
    if first_available != second_available:
        return {"agreement": "ONE_ENSEMBLE_ONLY", "pair": pair_label, "notes": ["ONE_ENSEMBLE_ONLY"]}
    first_stats = first_window.get("statistics") or {}
    second_stats = second_window.get("statistics") or {}

    def statistic(window_stats: dict, variable: str, field: str):
        return (window_stats.get(variable) or {}).get(field)

    pairs = [
        ("temperature_median_c", statistic(first_stats, "temperature_2m", "median"), statistic(second_stats, "temperature_2m", "median"), 5.0, "°C"),
        ("total_cloud_median_pct", statistic(first_stats, "cloud_cover", "median"), statistic(second_stats, "cloud_cover", "median"), 35.0, "%"),
        ("low_cloud_median_pct", statistic(first_stats, "cloud_cover_low", "median"), statistic(second_stats, "cloud_cover_low", "median"), 35.0, "%"),
        ("mid_cloud_median_pct", statistic(first_stats, "cloud_cover_mid", "median"), statistic(second_stats, "cloud_cover_mid", "median"), 35.0, "%"),
        ("high_cloud_median_pct", statistic(first_stats, "cloud_cover_high", "median"), statistic(second_stats, "cloud_cover_high", "median"), 35.0, "%"),
        ("dew_point_median_c", statistic(first_stats, "dew_point_2m", "median"), statistic(second_stats, "dew_point_2m", "median"), 4.0, "°C"),
        ("relative_humidity_median_pct", statistic(first_stats, "relative_humidity_2m", "median"), statistic(second_stats, "relative_humidity_2m", "median"), 25.0, "%"),
        ("precipitation_median_mm", statistic(first_stats, "precipitation", "median"), statistic(second_stats, "precipitation", "median"), 2.0, "mm"),
        ("snowfall_median_cm", statistic(first_stats, "snowfall", "median"), statistic(second_stats, "snowfall", "median"), 2.0, "cm"),
        ("gust_p90_kmh", statistic(first_stats, "wind_gusts_10m", "p90"), statistic(second_stats, "wind_gusts_10m", "p90"), 20.0, "km/h"),
        ("wind_speed_median_kmh", statistic(first_stats, "wind_speed_10m", "median"), statistic(second_stats, "wind_speed_10m", "median"), 15.0, "km/h"),
    ]
    deltas = {}
    notes = []
    agreement_points = []
    for name, first_value, second_value, tolerance, unit in pairs:
        if first_value is None or second_value is None:
            deltas[name] = {
                "comparison": "UNAVAILABLE",
                "value": None,
                "unit": unit,
                "reason": "VARIABLE_UNAVAILABLE_IN_ONE_ENSEMBLE",
            }
            continue
        delta = abs(float(first_value) - float(second_value))
        label = "HIGH" if delta <= tolerance else "MEDIUM" if delta <= tolerance * 2 else "LOW"
        agreement_points.append(label)
        deltas[name] = {"comparison": label, "value": round(delta, 3), "unit": unit, "tolerance": tolerance}
        if label == "LOW":
            notes.append(f"{name}:LARGE_DIFFERENCE")
    if not agreement_points:
        return {"agreement": "UNAVAILABLE", "pair": pair_label, "variables": deltas, "notes": ["NO_SHARED_VARIABLES"]}
    worst = "LOW" if "LOW" in agreement_points else "MEDIUM" if "MEDIUM" in agreement_points else "HIGH"
    return {
        "agreement": worst,
        "pair": pair_label,
        "variables": deltas,
        "notes": notes,
        "method": "independent comparison, no cross-model averaging",
    }


def _deterministic_vs_deterministic_consistency(first_window: dict, second_window: dict, *, pair_label: str) -> dict:
    """Compare two deterministic windows variable by variable. No averaging."""
    if first_window.get("status") != "OK" or second_window.get("status") != "OK":
        return {
            "agreement": "UNAVAILABLE",
            "pair": pair_label,
            "variables": {},
            "notes": ["ONE_DETERMINISTIC_WINDOW_UNAVAILABLE"],
        }
    pairs = [
        # Source keys must match the raw ``_deterministic_hourly_window`` field
        # names; only the compact view renames the temperature to ``temp_mean_c``.
        ("temperature_mean_c", "temperature_mean_c", 5.0),
        ("total_cloud_pct", "total_cloud_pct", 35.0),
        ("low_cloud_pct", "low_cloud_pct", 35.0),
        ("mid_cloud_pct", "mid_cloud_pct", 35.0),
        ("high_cloud_pct", "high_cloud_pct", 35.0),
        ("precipitation_mm", "precipitation_mm", 2.0),
        ("snowfall_cm", "snowfall_cm", 2.0),
        ("wind_speed_mean_kmh", "wind_speed_mean_kmh", 15.0),
        ("gust_max_kmh", "gust_max_kmh", 20.0),
    ]
    results = {}
    labels = []
    notes = []
    for key, source_key, tolerance in pairs:
        first_value = first_window.get(source_key)
        second_value = second_window.get(source_key)
        if first_value is None or second_value is None:
            results[key] = {"comparison": "UNAVAILABLE", "value": None, "reason": "VARIABLE_UNAVAILABLE_IN_ONE_MODEL"}
            continue
        delta = abs(float(first_value) - float(second_value))
        label = "HIGH" if delta <= tolerance else "MEDIUM" if delta <= tolerance * 2 else "LOW"
        labels.append(label)
        results[key] = {"comparison": label, "value": round(delta, 3), "tolerance": tolerance}
        if label == "LOW":
            notes.append(f"{key}:LARGE_DIFFERENCE")
    if not labels:
        return {"agreement": "UNAVAILABLE", "pair": pair_label, "variables": results, "notes": ["NO_SHARED_VARIABLES"]}
    worst = "LOW" if "LOW" in labels else "MEDIUM" if "MEDIUM" in labels else "HIGH"
    return {
        "agreement": worst,
        "pair": pair_label,
        "variables": results,
        "notes": notes,
        "method": "independent comparison, no cross-model averaging",
    }


def _model_consistency(
    hres_window: dict,
    gfs_window: dict,
    ec_window: dict,
    gefs_window: dict,
) -> dict:
    """The four model-pair comparisons requested by the unified variable system."""
    return {
        "ec_hres_vs_ec_ensemble": _deterministic_consistency(hres_window, ec_window),
        "gfs_deterministic_vs_gefs": _deterministic_consistency(gfs_window, gefs_window),
        "ec_hres_vs_gfs_deterministic": _deterministic_vs_deterministic_consistency(
            hres_window, gfs_window, pair_label="ec_hres_vs_gfs_deterministic"
        ),
        "ec_ensemble_vs_gefs": _ensemble_probability_consistency(
            ec_window, gefs_window, pair_label="ec_ensemble_vs_gefs"
        ),
        "averaging_policy": "NO_CROSS_MODEL_AVERAGING",
    }


def _ensemble_consensus(ec_window: dict, gefs_window: dict) -> dict:
    ec_available = ec_window.get("status") in {"OK", "PARTIAL"}
    gefs_available = gefs_window.get("status") in {"OK", "PARTIAL"}
    if not ec_available and not gefs_available:
        return {"agreement": "UNAVAILABLE", "notes": ["BOTH_ENSEMBLES_OUTSIDE_HORIZON_OR_UNAVAILABLE"]}
    if ec_available != gefs_available:
        return {"agreement": "ONE_ENSEMBLE_ONLY", "notes": ["ONE_ENSEMBLE_OUTSIDE_HORIZON_OR_UNAVAILABLE"]}
    ec_stats = ec_window.get("statistics") or {}
    gefs_stats = gefs_window.get("statistics") or {}
    pairs = (
        ("cloud_cover_gt_70pct", ec_stats.get("probabilities", {}).get("cloud_cover_gt_70pct"), gefs_stats.get("probabilities", {}).get("cloud_cover_gt_70pct")),
        ("precipitation_gt_0_5mm", ec_stats.get("probabilities", {}).get("precipitation_gt_0_5mm"), gefs_stats.get("probabilities", {}).get("precipitation_gt_0_5mm")),
        ("snowfall_gt_0_5cm", ec_stats.get("probabilities", {}).get("snowfall_gt_0_5cm"), gefs_stats.get("probabilities", {}).get("snowfall_gt_0_5cm")),
    )
    deltas = []
    notes = []
    for name, ec_item, gefs_item in pairs:
        if isinstance(ec_item, dict) and isinstance(gefs_item, dict) and ec_item.get("probability") is not None and gefs_item.get("probability") is not None:
            delta = abs(ec_item["probability"] - gefs_item["probability"])
            deltas.append(delta)
            if delta > 0.4:
                notes.append(f"{name}:LARGE_PROBABILITY_DIFFERENCE")
    if not deltas:
        return {"agreement": "UNAVAILABLE", "notes": ["NO_SHARED_PROBABILITY_FIELDS"]}
    maximum = max(deltas)
    agreement = "HIGH" if maximum <= 0.2 else "MEDIUM" if maximum <= 0.4 else "LOW"
    return {"agreement": agreement, "notes": notes, "max_probability_difference": round(maximum, 3)}


def _signal_level(probability: float | None, median: float | None, *, high: float, moderate: float) -> str:
    if probability is not None and probability >= high:
        return "HIGH"
    if probability is not None and probability >= moderate:
        return "MODERATE"
    if probability is None:
        if median is None:
            return "UNCERTAIN"
        return "HIGH" if median >= 70 else "MODERATE" if median >= 35 else "LOW"
    return "LOW"


def _viewing_signal(
    gefs_window: dict,
    ec_window: dict,
    consistency: dict,
    ensemble_agreement: str | None = None,
) -> dict:
    gefs_available = gefs_window.get("status") in {"OK", "PARTIAL"}
    ec_available = ec_window.get("status") in {"OK", "PARTIAL"}
    if not gefs_available and not ec_available:
        return {
            "cloud_signal": "UNCERTAIN",
            "total_cloud_signal": "UNCERTAIN",
            "low_cloud_signal": "UNCERTAIN",
            "mid_cloud_signal": "UNCERTAIN",
            "high_cloud_signal": "UNCERTAIN",
            "precip_signal": "UNCERTAIN",
            "snow_signal": "UNCERTAIN",
            "wind_signal": "UNCERTAIN",
            "visibility_related_signal": "UNCERTAIN",
            "model_agreement": "UNAVAILABLE",
            "layer_sources": {variable: "UNAVAILABLE" for variable in CLOUD_LAYER_VARIABLES},
            "signal_sources": {
                signal: "UNAVAILABLE" for signal, _, _ in VIEWING_SIGNAL_SOURCE_VARIABLES
            },
            "notes": ["NO_VIEWING_SOURCE_AVAILABLE"],
        }
    # GEFS is preferred for the GFS cross-check, but a quantity GEFS cannot
    # supply (the western Sichuan layered-cloud null-array case) is read from the
    # independent ECMWF ensemble instead of being reported as UNCERTAIN.  The
    # two ensembles are never averaged and the chosen source is published.
    gefs_stats = (gefs_window.get("statistics") or {}) if gefs_available else {}
    ec_stats = (ec_window.get("statistics") or {}) if ec_available else {}

    def probability(name: str) -> float | None:
        for stats in (gefs_stats, ec_stats):
            item = (stats.get("probabilities") or {}).get(name)
            if isinstance(item, dict) and item.get("probability") is not None:
                return item["probability"]
        return None

    def median(name: str) -> float | None:
        for stats in (gefs_stats, ec_stats):
            item = stats.get(name)
            if isinstance(item, dict) and item.get("median") is not None:
                return item["median"]
        return None

    def source_for(variable: str) -> str:
        for label, stats in (("gefs", gefs_stats), ("ecmwf_ensemble", ec_stats)):
            item = stats.get(variable)
            if isinstance(item, dict) and item.get("median") is not None:
                return label
        return "UNAVAILABLE"

    def signal_source(probability_name: str, variable: str) -> str:
        """Name the ensemble that actually supplied a published signal.

        ``probability`` and ``median`` both read GEFS first and fall back to the
        independent ECMWF ensemble, so a signal can come from either one.  The
        value is never an average of the two.
        """
        for label, stats in (("gefs", gefs_stats), ("ecmwf_ensemble", ec_stats)):
            item = (stats.get("probabilities") or {}).get(probability_name)
            if isinstance(item, dict) and item.get("probability") is not None:
                return label
        return source_for(variable)

    cloud = probability("cloud_cover_gt_70pct")
    low = probability("cloud_cover_low_gt_50pct")
    mid = probability("cloud_cover_mid_gt_50pct")
    high = probability("cloud_cover_high_gt_50pct")
    precip = probability("precipitation_gt_0_5mm")
    snow = probability("snowfall_gt_0_5cm")
    gust = probability("gust_gt_50kmh")

    median_cloud = median("cloud_cover")
    median_low = median("cloud_cover_low")
    median_mid = median("cloud_cover_mid")
    median_high = median("cloud_cover_high")
    median_humidity = median("relative_humidity_2m")

    cloud_signal = (
        "UNCERTAIN"
        if cloud is None and median_cloud is None
        else "CLOUDY"
        if (cloud is not None and cloud >= 0.65) or (median_cloud is not None and median_cloud >= 70)
        else "CLEAR"
        if (cloud is None or cloud <= 0.25) and (median_cloud is None or median_cloud < 35)
        else "MIXED"
    )
    low_signal = _signal_level(low, median_low, high=0.5, moderate=0.2)
    mid_signal = _signal_level(mid, median_mid, high=0.5, moderate=0.2)
    high_signal = _signal_level(high, median_high, high=0.5, moderate=0.2)
    precip_signal = "UNCERTAIN" if precip is None else "HIGH" if precip >= 0.6 else "MODERATE" if precip >= 0.3 else "LOW"
    snow_signal = "UNCERTAIN" if snow is None else "HIGH" if snow >= 0.5 else "MODERATE" if snow >= 0.2 else "LOW"
    wind_signal = "UNCERTAIN" if gust is None else "HIGH" if gust >= 0.5 else "MODERATE" if gust >= 0.25 else "LOW"

    # Obstruction risk only.  No numeric visibility is published because the
    # Open-Meteo hourly fields used here are not a visibility measurement.
    # Only low cloud, precipitation and snow fog can physically hide the
    # terrain.  Mid cloud does not obstruct a mountain view; it flattens direct
    # sunlight, which mid_cloud_signal reports separately.
    obstruction_inputs = [value for value in (low, precip) if value is not None]
    if not obstruction_inputs and median_low is None and median_humidity is None:
        visibility_signal = "UNCERTAIN"
    else:
        obstruction = max(obstruction_inputs) if obstruction_inputs else 0.0
        if (median_low is not None and median_low >= 60) or obstruction >= 0.6:
            visibility_signal = "HIGH"
        elif (median_low is not None and median_low >= 30) or obstruction >= 0.3:
            visibility_signal = "MODERATE"
        else:
            visibility_signal = "LOW"
    if gefs_available and ec_available:
        agreement = ensemble_agreement or "UNAVAILABLE"
    else:
        agreement = "SINGLE_ENSEMBLE"
    notes = ["HIGH_CLOUD_IS_NOT_AUTOMATICALLY_BAD_WEATHER"]
    if low_signal == "HIGH":
        notes.append("LOW_CLOUD_CAN_BLOCK_TERRAIN_VIEWS")
    if mid_signal in {"MODERATE", "HIGH"}:
        notes.append("MID_CLOUD_CAN_FLATTEN_DIRECT_SUNLIGHT")
    if high_signal in {"MODERATE", "HIGH"}:
        notes.append("HIGH_CLOUD_ADDS_SKY_TEXTURE_AND_SUNRISE_SUNSET_POTENTIAL")
    return {
        "cloud_signal": cloud_signal,
        "total_cloud_signal": cloud_signal,
        "low_cloud_signal": low_signal,
        "mid_cloud_signal": mid_signal,
        "high_cloud_signal": high_signal,
        "precip_signal": precip_signal,
        "snow_signal": snow_signal,
        "wind_signal": wind_signal,
        "visibility_related_signal": visibility_signal,
        "model_agreement": agreement,
        "layer_sources": {
            variable: source_for(variable)
            for variable in CLOUD_LAYER_VARIABLES
        },
        "signal_sources": {
            signal: signal_source(probability_name, variable)
            for signal, probability_name, variable in VIEWING_SIGNAL_SOURCE_VARIABLES
        },
        "notes": notes,
    }


def _target_forecast_granularity(forecast_date: dt.date, target_date: dt.date) -> str:
    lead = (target_date - forecast_date).days
    if lead <= 7:
        return "hourly_window_supported"
    if lead <= 14:
        return "day_window_only"
    return "trend_only"


def _gefs_phase_for_date(point_record: dict | None, target_date: dt.date, event_type: str) -> dict | None:
    segment_key, segment = _gefs_segment_for_date(point_record, target_date)
    if not segment:
        return None
    phase = copy.deepcopy((segment.get("event_phases") or {}).get(event_type))
    if not phase:
        return None
    phase["segment"] = segment_key
    distribution = phase.get("event_day_distribution") or {}
    phase["relevant_to_date"] = target_date.isoformat() in distribution
    return phase


def _compact_deterministic_view(view: dict) -> dict:
    if view.get("status") not in {"OK", "PARTIAL"}:
        return {
            "available": False,
            "status": view.get("status", "UNAVAILABLE"),
            "reason": view.get("reason", "DETERMINISTIC_DATA_UNAVAILABLE"),
        }
    return {
        "available": True,
        "status": view.get("status"),
        "cloud": view.get("total_cloud_pct"),
        "low_cloud": view.get("low_cloud_pct"),
        "mid_cloud": view.get("mid_cloud_pct"),
        "high_cloud": view.get("high_cloud_pct"),
        "precip_mm": view.get("precipitation_mm"),
        "rain_mm": view.get("rain_mm"),
        "snow_cm": view.get("snowfall_cm"),
        "gust_kmh": view.get("gust_max_kmh"),
        "wind_speed_kmh": view.get("wind_speed_mean_kmh"),
        "wind_direction_deg": view.get("wind_direction_mean_deg"),
        "wind_direction_resultant_length": view.get("wind_direction_resultant_length"),
        "temp_min_c": view.get("temperature_min_c"),
        "temp_mean_c": view.get("temperature_mean_c"),
        "temp_max_c": view.get("temperature_max_c"),
        "dew_point_c": view.get("dew_point_mean_c"),
        "relative_humidity_pct": view.get("relative_humidity_mean_pct"),
        "sunshine_or_shortwave": view.get("sunshine_or_shortwave"),
    }


def _compact_ensemble_view(view: dict) -> dict:
    if view.get("status") not in {"OK", "PARTIAL"}:
        return {
            "available": False,
            "status": view.get("status", "UNAVAILABLE"),
            "reason": view.get("reason", "ENSEMBLE_DATA_UNAVAILABLE"),
        }
    stats = view.get("statistics") or {}
    probabilities = stats.get("probabilities") or {}

    def statistic(name: str, field: str) -> object:
        return (stats.get(name) or {}).get(field)

    def probability(name: str) -> object:
        return (probabilities.get(name) or {}).get("probability")

    def available(name: str) -> bool:
        return (stats.get(name) or {}).get("available_members", 0) > 0

    return {
        "available": True,
        "status": view.get("status"),
        "model_id": view.get("model_id"),
        "resolution": view.get("resolution"),
        "members_valid": view.get("members_valid"),
        "cloud_median": statistic("cloud_cover", "median"),
        "low_cloud_median": statistic("cloud_cover_low", "median"),
        "mid_cloud_median": statistic("cloud_cover_mid", "median"),
        "high_cloud_median": statistic("cloud_cover_high", "median"),
        "p_cloud_gt_50": probability("cloud_cover_gt_50pct"),
        "p_cloud_gt_70": probability("cloud_cover_gt_70pct"),
        "p_low_cloud_gt_50": probability("cloud_cover_low_gt_50pct"),
        "p_mid_cloud_gt_50": probability("cloud_cover_mid_gt_50pct"),
        "p_high_cloud_gt_50": probability("cloud_cover_high_gt_50pct"),
        "p_precip": probability("precipitation_gt_0_5mm"),
        "p_snow": probability("snowfall_gt_0_5cm"),
        "wind_speed_median": statistic("wind_speed_10m", "median"),
        "gust_median": statistic("wind_gusts_10m", "median"),
        "gust_p90": statistic("wind_gusts_10m", "p90"),
        "temp_p10": statistic("temperature_2m", "p10"),
        "temp_median": statistic("temperature_2m", "median"),
        "temp_p90": statistic("temperature_2m", "p90"),
        "dew_point_median": statistic("dew_point_2m", "median"),
        "relative_humidity_median": statistic("relative_humidity_2m", "median"),
        # Explicit availability so a null median is never confused with a real
        # value, and so layered cloud cannot be replaced by a total-cloud guess.
        "layer_availability": {
            name: available(name)
            for name in ("cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high")
        },
    }


def _compact_phase(phase: dict | None) -> dict:
    if not phase:
        return {"available": False, "status": "UNAVAILABLE"}
    if phase.get("status") != "SIGNAL":
        return {
            "available": False,
            "status": phase.get("status", "NO_SIGNAL"),
            "phase_confidence": phase.get("phase_confidence"),
            "phase_spread_hours": phase.get("phase_spread_hours"),
            "multimodal": bool(phase.get("multimodal")),
            "relevant_to_date": phase.get("relevant_to_date", False),
        }

    def median_time(name: str) -> str | None:
        return ((phase.get(name) or {}).get("median"))

    return {
        "available": True,
        "status": "SIGNAL",
        "start": median_time("event_start"),
        "peak": median_time("event_peak"),
        "end": median_time("event_end"),
        "members_with_event": phase.get("members_with_event"),
        "member_support": phase.get("member_support"),
        "phase_spread_hours": phase.get("phase_spread_hours"),
        "phase_confidence": phase.get("phase_confidence"),
        "multimodal": bool(phase.get("multimodal")),
        "relevant_to_date": phase.get("relevant_to_date", False),
    }


def _local_index_map(times: list[str]) -> dict[dt.datetime, int]:
    mapping: dict[dt.datetime, int] = {}
    for index, value in enumerate(times):
        try:
            mapping[parse_local_api_time(value)] = index
        except (TypeError, ValueError):
            continue
    return mapping


def _hourly_slice(
    hourly: dict,
    mapping: dict[dt.datetime, int],
    variable: str,
    start: dt.datetime,
    end: dt.datetime,
    *,
    reducer: str = "mean",
) -> float | None:
    series = hourly.get(variable)
    if not isinstance(series, list):
        return None
    values = [
        float(series[index])
        for moment, index in mapping.items()
        if start <= moment < end and index < len(series) and series[index] is not None
    ]
    if not values:
        return None
    if reducer == "sum":
        return round(sum(values), 3)
    if reducer == "min":
        return round(min(values), 3)
    if reducer == "max":
        return round(max(values), 3)
    return round(mean(values), 3)


def _fog_inputs(record: dict | None, target_date: dt.date, cutoff_date: dt.date) -> dict:
    """Raw pre-dawn indicators for the morning-fog question.

    Only observed forecast quantities are published.  No fabricated fog
    probability is produced; downstream consumers read the signals and the raw
    metrics themselves.
    """
    unavailable = {
        "status": "UNAVAILABLE",
        "reason": "DETERMINISTIC_MODULE_UNAVAILABLE",
        "moisture_signal": "UNCERTAIN",
        "radiative_cooling_signal": "UNCERTAIN",
        "wind_signal": "UNCERTAIN",
        "system_low_cloud_risk": "UNCERTAIN",
        "probability_published": False,
    }
    if not record or record.get("status") != "PASS":
        return unavailable
    hourly = record.get("hourly") or {}
    times = hourly.get("time") if isinstance(hourly.get("time"), list) else []
    mapping = _local_index_map(times)
    if not mapping:
        return dict(unavailable, reason="HOURLY_SERIES_EMPTY")
    morning_end = dt.datetime.combine(target_date, dt.time(8, 0), tzinfo=LOCAL_TZ)
    night_start = dt.datetime.combine(target_date - dt.timedelta(days=1), dt.time(18, 0), tzinfo=LOCAL_TZ)
    twelve_start = dt.datetime.combine(target_date - dt.timedelta(days=1), dt.time(20, 0), tzinfo=LOCAL_TZ)
    twenty_four_start = dt.datetime.combine(target_date - dt.timedelta(days=1), dt.time(8, 0), tzinfo=LOCAL_TZ)
    pre_dawn_start = dt.datetime.combine(target_date, dt.time(FOG_PRE_DAWN_START_HOUR, 0), tzinfo=LOCAL_TZ)
    if morning_end.date() > cutoff_date + dt.timedelta(days=1):
        return dict(unavailable, reason="OUTSIDE_FORECAST_HORIZON")

    night_temp_mean = _hourly_slice(hourly, mapping, "temperature_2m", night_start, morning_end)
    night_temp_min = _hourly_slice(hourly, mapping, "temperature_2m", night_start, morning_end, reducer="min")
    night_dew_point = _hourly_slice(hourly, mapping, "dew_point_2m", night_start, morning_end)
    night_humidity = _hourly_slice(hourly, mapping, "relative_humidity_2m", night_start, morning_end)
    night_total_cloud = _hourly_slice(hourly, mapping, "cloud_cover", night_start, morning_end)
    night_low_cloud = _hourly_slice(hourly, mapping, "cloud_cover_low", night_start, morning_end)
    night_mid_cloud = _hourly_slice(hourly, mapping, "cloud_cover_mid", night_start, morning_end)
    night_high_cloud = _hourly_slice(hourly, mapping, "cloud_cover_high", night_start, morning_end)
    pre_dawn_wind = _hourly_slice(hourly, mapping, "wind_speed_10m", pre_dawn_start, morning_end)
    pre_dawn_gust = _hourly_slice(hourly, mapping, "wind_gusts_10m", pre_dawn_start, morning_end, reducer="max")
    previous_12h_precip = _hourly_slice(hourly, mapping, "precipitation", twelve_start, morning_end, reducer="sum")
    previous_24h_precip = _hourly_slice(hourly, mapping, "precipitation", twenty_four_start, morning_end, reducer="sum")
    dew_point_spread = (
        round(night_temp_mean - night_dew_point, 3)
        if night_temp_mean is not None and night_dew_point is not None
        else None
    )

    moisture_components = []
    if previous_12h_precip is not None:
        moisture_components.append(previous_12h_precip > 0.2 or previous_24h_precip is not None and previous_24h_precip > 1.0)
    if night_humidity is not None:
        moisture_components.append(night_humidity >= 85)
    if night_humidity is None and previous_12h_precip is None:
        moisture_signal = "UNCERTAIN"
    elif all(moisture_components):
        moisture_signal = "HIGH"
    elif any(moisture_components):
        moisture_signal = "MODERATE"
    else:
        moisture_signal = "LOW"

    if dew_point_spread is None:
        radiative_cooling_signal = "UNCERTAIN"
    elif dew_point_spread <= 1.0:
        radiative_cooling_signal = "HIGH"
    elif dew_point_spread <= FOG_RADIATIVE_COOLING_SPREAD_C:
        radiative_cooling_signal = "MODERATE"
    else:
        radiative_cooling_signal = "LOW"

    if pre_dawn_wind is None:
        wind_signal = "UNCERTAIN"
    elif pre_dawn_wind <= FOG_WIND_CALM_KMH:
        wind_signal = "CALM"
    elif pre_dawn_wind <= FOG_WIND_BREAKUP_KMH:
        wind_signal = "LIGHT"
    else:
        wind_signal = "MIXING"

    if night_low_cloud is None:
        system_low_cloud_risk = "UNCERTAIN"
    elif night_low_cloud >= 60:
        system_low_cloud_risk = "HIGH"
    elif night_low_cloud >= 30:
        system_low_cloud_risk = "MODERATE"
    else:
        system_low_cloud_risk = "LOW"

    signals = [moisture_signal, radiative_cooling_signal, system_low_cloud_risk]
    status = "PARTIAL" if "UNCERTAIN" in signals else "OK"
    return {
        "status": status,
        "reason": None if status == "OK" else "SOME_FOG_INPUTS_UNAVAILABLE",
        "date": target_date.isoformat(),
        "night_window": {
            "start": night_start.isoformat(),
            "end": morning_end.isoformat(),
            "definition": "previous day 18:00 local to target day 08:00 local",
        },
        "previous_12h_precip_mm": previous_12h_precip,
        "previous_24h_precip_mm": previous_24h_precip,
        "night_relative_humidity": night_humidity,
        "night_dew_point": night_dew_point,
        "night_temp": night_temp_mean,
        "night_temp_min": night_temp_min,
        "night_temp_dewpoint_spread": dew_point_spread,
        "pre_dawn_wind_speed": pre_dawn_wind,
        "pre_dawn_gust": pre_dawn_gust,
        "night_total_cloud": night_total_cloud,
        "night_low_cloud": night_low_cloud,
        "night_mid_cloud": night_mid_cloud,
        "night_high_cloud": night_high_cloud,
        "moisture_signal": moisture_signal,
        "radiative_cooling_signal": radiative_cooling_signal,
        "wind_signal": wind_signal,
        "system_low_cloud_risk": system_low_cloud_risk,
        "probability_published": False,
        "interpretation": "Raw indicators only; no fog probability is produced.",
    }


def _target_window_location(
    config: dict,
    point_id: str,
    target_date: dt.date,
    forecast_date: dt.date,
    hres: dict,
    gfs: dict,
    ensemble: dict,
    gefs: dict,
    cutoff_date: dt.date,
) -> dict:
    point = active_points(config).get(point_id)
    name = point.get("name") if point else point_id
    hres_record = (hres.get("points") or {}).get(point_id)
    gfs_record = (gfs.get("points") or {}).get(point_id)
    ec_record = (ensemble.get("points") or {}).get(point_id)
    # The ECMWF ensemble is fetched for each region's core point only.  A summary
    # node outside that set borrows its own region's core grid rather than
    # reporting the ensemble as unavailable, and names the borrowed point so the
    # reference grid is never mistaken for a point-level ensemble.
    ensemble_reference_point_id = None
    if ec_record is None and point:
        region = (config.get("regions") or {}).get(point.get("region")) or {}
        candidate = region.get("core_point_id")
        if candidate and candidate != point_id:
            candidate_record = (ensemble.get("points") or {}).get(candidate)
            if candidate_record is not None:
                ec_record = candidate_record
                ensemble_reference_point_id = candidate
    gefs_record = (gefs.get("points") or {}).get(point_id)
    hres_daily = _deterministic_daily_summary(hres_record, target_date, cutoff_date)
    gfs_daily = _deterministic_daily_summary(gfs_record, target_date, cutoff_date)
    hres_windows = {window: _deterministic_hourly_window(hres_record, target_date, window, cutoff_date) for window in TARGET_WINDOW_NAMES}
    gfs_windows = {window: _deterministic_hourly_window(gfs_record, target_date, window, cutoff_date) for window in TARGET_WINDOW_NAMES}
    ec_windows = {window: _ensemble_window_view(ec_record, target_date, window, cutoff_date) for window in TARGET_WINDOW_NAMES}
    gefs_windows = {window: _gefs_window_view(gefs_record, target_date, window) for window in TARGET_WINDOW_NAMES}
    consistency = {
        window: _deterministic_consistency(gfs_windows[window], gefs_windows[window])
        for window in TARGET_WINDOW_NAMES
    }
    model_consistency = {
        window: _model_consistency(
            hres_windows[window],
            gfs_windows[window],
            ec_windows[window],
            gefs_windows[window],
        )
        for window in TARGET_WINDOW_NAMES
    }
    phases = {
        name: _gefs_phase_for_date(gefs_record, target_date, name)
        for name in ("CLOUD_EVENT", "PRECIP_EVENT", "SNOW_EVENT", "COLD_EVENT")
    }
    consensus = {
        window: _ensemble_consensus(ec_windows[window], gefs_windows[window])
        for window in TARGET_WINDOW_NAMES
    }
    viewing = {
        window.lower(): _viewing_signal(
            gefs_windows[window],
            ec_windows[window],
            consistency[window],
            consensus[window].get("agreement"),
        )
        for window in TARGET_WINDOW_NAMES
    }
    event_phase = {
        "cloud_window": _compact_phase(phases["CLOUD_EVENT"]),
        "precip_window": _compact_phase(phases["PRECIP_EVENT"]),
        "snow_window": _compact_phase(phases["SNOW_EVENT"]),
        "cold_window": _compact_phase(phases["COLD_EVENT"]),
        "phase_spread_hours": {
            key: (value or {}).get("phase_spread_hours")
            for key, value in (
                ("cloud", phases["CLOUD_EVENT"]),
                ("precip", phases["PRECIP_EVENT"]),
                ("snow", phases["SNOW_EVENT"]),
                ("cold", phases["COLD_EVENT"]),
            )
        },
        "phase_confidence": {
            key: (value or {}).get("phase_confidence")
            for key, value in (
                ("cloud", phases["CLOUD_EVENT"]),
                ("precip", phases["PRECIP_EVENT"]),
                ("snow", phases["SNOW_EVENT"]),
                ("cold", phases["COLD_EVENT"]),
            )
        },
        "multimodal": {
            key: bool((value or {}).get("multimodal"))
            for key, value in (
                ("cloud", phases["CLOUD_EVENT"]),
                ("precip", phases["PRECIP_EVENT"]),
                ("snow", phases["SNOW_EVENT"]),
                ("cold", phases["COLD_EVENT"]),
            )
        },
    }

    def compact_window(window: str) -> dict:
        agreement = consensus[window].get("agreement", "UNAVAILABLE")
        result = {
            "ec_det": _compact_deterministic_view(hres_windows[window]),
            "gfs_det": _compact_deterministic_view(gfs_windows[window]),
            "ec_ens": _compact_ensemble_view(ec_windows[window]),
            "gefs": _compact_ensemble_view(gefs_windows[window]),
            "gfs_support": consistency[window],
            "gfs_deterministic_vs_gefs": consistency[window],
            "model_consistency": model_consistency[window],
            "ensemble_agreement": agreement,
            "viewing_conditions": viewing[window.lower()],
        }
        if consensus[window].get("notes"):
            result["ensemble_agreement_notes"] = consensus[window]["notes"]
        return result

    return {
        "location_id": point_id,
        "location_name": name,
        "usable_for_main_chain": bool(point and point.get("status") == "VERIFIED"),
        "ensemble_reference_point_id": ensemble_reference_point_id,
        "forecast_granularity": _target_forecast_granularity(forecast_date, target_date),
        "daily": {
            "ec_det": _compact_deterministic_view(hres_daily),
            "gfs_det": _compact_deterministic_view(gfs_daily),
        },
        "morning": compact_window("MORNING"),
        "afternoon": compact_window("AFTERNOON"),
        "night": compact_window("NIGHT"),
        "fog_inputs": _fog_inputs(hres_record, target_date, cutoff_date),
        "event_phase": event_phase,
    }


def build_target_window_brief(
    config: dict,
    forecast_date: dt.date,
    hres: dict,
    gfs: dict,
    ensemble: dict,
    gefs: dict,
    cutoff_date: dt.date = GEFS_TRAVEL_CUTOFF_DATE,
    generated_at: str | None = None,
) -> dict:
    itinerary = {
        "2026-10-24": {
            "locations": ["SQG_SHUANGQIAO", "BPG_CENTER", "LXL_HIGH_PASS"],
            "priority_windows": ["MORNING", "AFTERNOON"],
        },
        "2026-10-25": {
            "locations": ["SQG_SHUANGQIAO", "BPG_CENTER", "LXL_HIGH_PASS"],
            "priority_windows": ["MORNING"],
        },
        "2026-10-31": {
            "locations": ["JZG_TREESHENG", "JZG_NORILANG", "JZG_PRIMEVAL", "JZG_LONGHAI"],
            "priority_windows": ["MORNING", "AFTERNOON"],
        },
        "2026-11-01": {
            "locations": ["JZG_TREESHENG", "JZG_NORILANG", "JZG_PRIMEVAL", "JZG_LONGHAI"],
            "priority_windows": ["MORNING"],
        },
    }
    active = active_points(config)
    # The itinerary brief spans the Siguniang weekend (10/24-25) and the
    # Jiuzhaigou weekend (10/31-11/01) as one continuous target window.
    first_target_date = dt.date(2026, 10, 24)
    dates = [first_target_date + dt.timedelta(days=offset) for offset in range(9)]
    days = {}
    for target_date in dates:
        day_items = {}
        # The brief is itinerary-facing: keep only verified points actually
        # assigned to that date.  The full formal point registry remains in
        # gefs.json and the other detail artifacts.
        point_ids = itinerary.get(target_date.isoformat(), {}).get("locations", [])
        for point_id in point_ids:
            if point_id not in active:
                continue
            item = _target_window_location(config, point_id, target_date, forecast_date, hres, gfs, ensemble, gefs, cutoff_date)
            day_items[point_id] = item
        days[target_date.isoformat()] = day_items

    def dates_for(predicate) -> list[str]:
        found = []
        for target_date in dates:
            day_items = days[target_date.isoformat()].values()
            if any(predicate(item) for item in day_items):
                found.append(target_date.isoformat())
        return found

    def daily_value(target_date: dt.date, model_key: str) -> list[float]:
        values = []
        for item in days[target_date.isoformat()].values():
            value = ((item.get("daily") or {}).get(model_key) or {}).get("cloud")
            if value is not None:
                values.append(value)
        return values

    clearest_ec = sorted(
        dates,
        key=lambda value: mean(daily_value(value, "ec_det")) if daily_value(value, "ec_det") else 999,
    )[:3]
    clearest_gfs = sorted(
        dates,
        key=lambda value: mean(daily_value(value, "gfs_det")) if daily_value(value, "gfs_det") else 999,
    )[:3]
    return {
        "status": "OK",
        "generated_at": generated_at,
        "forecast_date": forecast_date.isoformat(),
        "cutoff_date": cutoff_date.isoformat(),
        "dates": [value.isoformat() for value in dates],
        "days": days,
        "itinerary_focus": itinerary,
        "window_overview": {
            "main_weather_window": {"start_date": dates[0].isoformat(), "end_date": dates[-1].isoformat(), "granularity": "date_and_observation_window"},
            "cold_air_window": dates_for(lambda item: ((item.get("event_phase") or {}).get("cold_window") or {}).get("relevant_to_date") is True),
            "precip_window": dates_for(lambda item: ((item.get("event_phase") or {}).get("precip_window") or {}).get("relevant_to_date") is True),
            "snow_window": dates_for(lambda item: ((item.get("event_phase") or {}).get("snow_window") or {}).get("relevant_to_date") is True),
            "clearest_days_ec": [value.isoformat() for value in clearest_ec],
            "clearest_days_gfs": [value.isoformat() for value in clearest_gfs],
            "ensemble_best_supported_clear_windows": dates_for(
                lambda item: any(
                    view.get("viewing_conditions", {}).get("cloud_signal") == "CLEAR"
                    and view.get("ensemble_agreement") in {"HIGH", "MEDIUM"}
                    for view in (item.get("morning"), item.get("afternoon"))
                    if view
                )
            ),
            "highest_low_cloud_risk_windows": dates_for(
                lambda item: any(
                    (view.get("viewing_conditions") or {}).get("low_cloud_signal") == "HIGH"
                    for view in (item.get("morning"), item.get("afternoon"), item.get("night"))
                    if view
                )
            ),
            "highest_mid_cloud_flat_light_windows": dates_for(
                lambda item: any(
                    (view.get("viewing_conditions") or {}).get("mid_cloud_signal") == "HIGH"
                    for view in (item.get("morning"), item.get("afternoon"))
                    if view
                )
            ),
            "best_high_cloud_texture_windows": dates_for(
                lambda item: any(
                    (view.get("viewing_conditions") or {}).get("high_cloud_signal") in {"MODERATE", "HIGH"}
                    for view in (item.get("morning"), item.get("afternoon"))
                    if view
                )
            ),
            "low_visibility_related_risk_windows": dates_for(
                lambda item: any(
                    (view.get("viewing_conditions") or {}).get("visibility_related_signal") == "HIGH"
                    for view in (item.get("morning"), item.get("afternoon"))
                    if view
                )
            ),
            "fog_favourable_dates": dates_for(
                lambda item: (
                    (item.get("fog_inputs") or {}).get("moisture_signal") in {"MODERATE", "HIGH"}
                    and (item.get("fog_inputs") or {}).get("radiative_cooling_signal") in {"MODERATE", "HIGH"}
                    and (item.get("fog_inputs") or {}).get("wind_signal") in {"CALM", "LIGHT"}
                )
            ),
            "highest_wind_risk_windows": dates_for(
                lambda item: any(
                    (view.get("viewing_conditions") or {}).get("wind_signal") == "HIGH"
                    for view in (item.get("morning"), item.get("afternoon"))
                    if view
                )
            ),
            "highest_snow_risk_windows": dates_for(
                lambda item: any(
                    (view.get("viewing_conditions") or {}).get("snow_signal") == "HIGH"
                    for view in (item.get("morning"), item.get("afternoon"))
                    if view
                )
            ),
            "largest_model_disagreement_dates": dates_for(
                lambda item: any(
                    view.get("ensemble_agreement") == "LOW"
                    for view in (item.get("morning"), item.get("afternoon"))
                    if view
                )
            ),
            "single_ensemble_only_dates": dates_for(
                lambda item: any(
                    view.get("ensemble_agreement") == "ONE_ENSEMBLE_ONLY"
                    for view in (item.get("morning"), item.get("afternoon"))
                    if view
                )
            ),
        },
        "verified_location_ids": sorted(active),
        "excluded_location_ids": sorted(excluded_points(config)),
        "interpretation_boundary": "Machine-readable weather and model-consistency summary for 2026-10-24 through 2026-11-01; no phenology or travel conclusion.",
    }


def build_phenology_weather_summary(
    config: dict,
    generated_at: str,
    data_date: str,
    forecast_date: dt.date,
    hres: dict,
    history_forward: dict,
    weather_events: dict | None = None,
) -> dict:
    """Build the compact machine-readable weather-only statistics artifact."""
    regions = {}
    for region_id in CORE_REGION_IDS:
        region_config = config.get("regions", {}).get(region_id, {})
        if region_id in SUBREGION_KEYS_BY_REGION and region_subregion_registry(config, region_id):
            regions[region_id] = build_registered_light_region(
                config,
                region_id,
                SUBREGION_KEYS_BY_REGION[region_id],
                generated_at,
                data_date,
                forecast_date,
                hres,
                history_forward,
            )
            if weather_events is not None:
                regions[region_id]["weather_events"] = weather_event_light_summary(weather_events, region_id)
            continue
        core_id = region_config.get("core_point_id")
        point = active_points(config).get(core_id) if core_id else None
        if not point:
            regions[region_id] = {
                "name": region_config.get("name"),
                "usable_for_main_chain": False,
                "status": "UNAVAILABLE",
                "reason": "NO_VERIFIED_CORE_POINT",
            }
            if weather_events is not None:
                regions[region_id]["weather_events"] = weather_event_light_summary(weather_events, region_id)
            continue
        years, sampling = point_year_lightweight_views(config, core_id, hres, history_forward, forecast_date)
        regions[region_id] = {
            "name": region_config.get("name"),
            "point_id": core_id,
            "usable_for_main_chain": True,
            "status": "OK" if sampling.get("status") == "OK" else "PARTIAL",
            "sampling": sampling,
            "years": years,
        }
        if weather_events is not None:
            regions[region_id]["weather_events"] = weather_event_light_summary(weather_events, region_id)
    return {
        "schema_version": SCHEMA_VERSION,
        "module": "phenology_weather_summary",
        "generated_at": generated_at,
        "data_date": data_date,
        "forecast_date": forecast_date.isoformat(),
        "source": "Open-Meteo",
        "model_policy": {
            "historical": "ECMWF IFS historical weather / analysis via archive-api; models=ecmwf_ifs",
            "forecast_2026": "ECMWF IFS HRES 9 km via forecast API",
            "timezone": TIMEZONE_NAME,
            "cell_selection": "nearest",
            "elevation": "nan",
            "grid_deduplication": "returned_grid_coordinate; one independent sample per returned model grid",
            "siguniang_aggregation": "unique grids equal within shuangqiao/bipenggou, then the two subregions equal in composite",
            "jiuzhaigou_aggregation": "unique grids equal within shuzheng/rize/zezhawa, then the three subregions equal in composite",
        },
        "window_definitions": history_forward_windows_for_year(forecast_date, 2026),
        "weather_events_path": "data/latest/weather_events.json",
        "weather_only": True,
        "interpretation_boundary": "Weather statistics only; this file contains no downstream ecological or travel conclusion.",
        "regions": regions,
    }


def summary_qa(
    hres_record: dict | None,
    history_region: dict | None,
    ensemble_record: dict | None,
    gfs_record: dict | None,
    single_region: dict | None,
    spatial_region: dict | None,
    long_range_region: dict | None = None,
    weather_events_region: dict | None = None,
) -> dict:
    result = {
        "hres": hres_record.get("status") if hres_record else "FAILED",
        "history": history_region.get("status", "FAILED") if history_region else "FAILED",
        "ensemble": ensemble_record.get("status") if ensemble_record else "FAILED",
        "gfs": gfs_record.get("status") if gfs_record else "FAILED",
        "single_runs": single_region.get("status") if single_region else "FAILED",
        "spatial_sampling": spatial_region.get("status") if spatial_region else "FAILED",
        "long_range": (
            long_range_region.get("status")
            if long_range_region
            else "FAILED"
        ),
    }
    if weather_events_region is not None:
        result["weather_events"] = weather_events_region.get("status", "FAILED")
    return result


# ---------------------------------------------------------------------------
# Three-year historical comparison
# ---------------------------------------------------------------------------
def historical_policy(config: dict) -> tuple[list[int], int]:
    policy = config.get("historical_comparison")
    if not isinstance(policy, dict):
        raise ValueError("historical_comparison policy is required")
    years = policy.get("years")
    window_days = policy.get("window_days_each_side")
    if (
        not isinstance(years, list)
        or len(years) != 3
        or any(isinstance(year, bool) or not isinstance(year, int) for year in years)
        or len(set(years)) != len(years)
    ):
        raise ValueError("historical_comparison.years must contain three unique integer years")
    if isinstance(window_days, bool) or not isinstance(window_days, int) or window_days < 0 or window_days > 14:
        raise ValueError("historical_comparison.window_days_each_side must be an integer from 0 to 14")
    return years, window_days


def historical_windows(config: dict) -> tuple[dict, dict]:
    years, window_days = historical_policy(config)
    groups = {}
    global_windows = {}
    for group_id, group in config["target_groups"].items():
        by_year = {}
        for year in years:
            target_dates = [
                dt.date.fromisoformat(raw_date).replace(year=year)
                for raw_date in group["dates"]
            ]
            start = min(target_dates) - dt.timedelta(days=window_days)
            end = max(target_dates) + dt.timedelta(days=window_days)
            by_year[str(year)] = {
                "start": start.isoformat(),
                "end": end.isoformat(),
                "target_dates": [value.isoformat() for value in target_dates],
            }
            current = global_windows.setdefault(str(year), {"start": start.isoformat(), "end": end.isoformat()})
            current["start"] = min(current["start"], start.isoformat())
            current["end"] = max(current["end"], end.isoformat())
        groups[group_id] = {
            "name": group["name"],
            "point_ids": group["point_ids"],
            "window_days_each_side": window_days,
            "by_year": by_year,
        }
    return groups, global_windows


def historical_request_params(config: dict, start_date: str, end_date: str) -> dict[str, object]:
    points = active_points(config)
    return {
        "latitude": ",".join(str(point["latitude"]) for point in points.values()),
        "longitude": ",".join(str(point["longitude"]) for point in points.values()),
        "timezone": TIMEZONE_NAME,
        "cell_selection": "nearest",
        "elevation": ",".join("nan" for _ in points),
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(HISTORICAL_DAILY_VARIABLES),
        "hourly": ",".join(HISTORICAL_HOURLY_VARIABLES),
    }


def historical_daily_rows(payload: dict) -> list[dict]:
    daily = payload.get("daily") if isinstance(payload.get("daily"), dict) else {}
    dates = daily.get("time") if isinstance(daily.get("time"), list) else []
    if not dates:
        return []
    lengths = [len(values) for key, values in daily.items() if key != "time" and isinstance(values, list)]
    if lengths and any(length != len(dates) for length in lengths):
        raise OpenMeteoError("HISTORICAL_DAILY_ARRAY_LENGTH_MISMATCH")

    cloud_by_date: dict[str, list[float]] = {}
    daypart_by_date: dict[str, dict[str, dict[str, object]]] = {}
    hourly = payload.get("hourly") if isinstance(payload.get("hourly"), dict) else {}
    hourly_times = hourly.get("time") if isinstance(hourly.get("time"), list) else []
    hourly_values = {
        variable: hourly.get(variable) if isinstance(hourly.get(variable), list) else []
        for variable in HISTORICAL_HOURLY_VARIABLES
    }
    for index, raw_time in enumerate(hourly_times):
        try:
            parsed_time = parse_local_api_time(raw_time)
            day = parsed_time.date().isoformat()
            hour = parsed_time.hour
        except (TypeError, ValueError):
            continue
        daypart = next(
            (name for name, start_hour, end_hour in HISTORICAL_DAYPARTS if start_hour <= hour < end_hour),
            None,
        )
        if daypart is None:
            continue
        bucket = daypart_by_date.setdefault(day, {}).setdefault(
            daypart,
            {
                "precipitation_mm": 0.0,
                "rain_mm": 0.0,
                "snowfall_cm": 0.0,
                "precipitation_hours": 0,
                "cloud_values": [],
                "hours_available": 0,
            },
        )
        bucket["hours_available"] += 1
        for variable, output_key in (
            ("precipitation", "precipitation_mm"),
            ("rain", "rain_mm"),
            ("snowfall", "snowfall_cm"),
        ):
            values = hourly_values[variable]
            value = values[index] if index < len(values) else None
            if value is None:
                continue
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                continue
            bucket[output_key] += numeric
            if variable == "precipitation" and numeric > 0:
                bucket["precipitation_hours"] += 1
        cloud_values = hourly_values["cloud_cover"]
        cloud_value = cloud_values[index] if index < len(cloud_values) else None
        if cloud_value is not None:
            try:
                numeric_cloud = float(cloud_value)
            except (TypeError, ValueError):
                numeric_cloud = None
            if numeric_cloud is not None:
                cloud_by_date.setdefault(day, []).append(numeric_cloud)
                bucket["cloud_values"].append(numeric_cloud)

    mappings = {
        "temperature_2m_mean": "temperature_mean_c",
        "temperature_2m_min": "temperature_min_c",
        "temperature_2m_max": "temperature_max_c",
        "precipitation_sum": "precipitation_mm",
        "rain_sum": "rain_mm",
        "snowfall_sum": "snowfall_cm",
        "precipitation_hours": "precipitation_hours",
    }
    rows = []
    for index, raw_date in enumerate(dates):
        row = {"date": raw_date}
        for source_key, output_key in mappings.items():
            values = daily.get(source_key)
            value = values[index] if isinstance(values, list) and index < len(values) else None
            row[output_key] = round_or_none(value)
        dayparts = {}
        for daypart, _, _ in HISTORICAL_DAYPARTS:
            bucket = daypart_by_date.get(raw_date, {}).get(daypart, {})
            cloud_values = bucket.get("cloud_values", [])
            dayparts[daypart] = {
                "precipitation_mm": round(float(bucket.get("precipitation_mm", 0.0)), 3),
                "rain_mm": round(float(bucket.get("rain_mm", 0.0)), 3),
                "snowfall_cm": round(float(bucket.get("snowfall_cm", 0.0)), 3),
                "precipitation_hours": int(bucket.get("precipitation_hours", 0)),
                "cloud_cover_mean_pct": round(mean(cloud_values), 3) if cloud_values else None,
                "hours_available": int(bucket.get("hours_available", 0)),
            }
        row["dayparts"] = dayparts
        cloud_values = cloud_by_date.get(raw_date, [])
        row["cloud_cover_mean_pct"] = round(mean(cloud_values), 3) if cloud_values else None
        row["cloud_cover_hours_available"] = len(cloud_values)
        rows.append(row)
    return rows


def historical_window_summary(rows: list[dict]) -> dict:
    def values(key: str) -> list[float]:
        return [float(row[key]) for row in rows if isinstance(row.get(key), (int, float))]

    precipitation = values("precipitation_mm")
    temperatures = values("temperature_mean_c")
    temperature_mins = values("temperature_min_c")
    temperature_maxes = values("temperature_max_c")
    cloud = values("cloud_cover_mean_pct")

    def count_above(threshold: float) -> int | None:
        return sum(value > threshold for value in precipitation) if precipitation else None

    def fraction_above(threshold: float) -> float | None:
        count = count_above(threshold)
        return round(count / len(precipitation), 3) if count is not None else None

    total_precipitation = sum(precipitation) if precipitation else None
    precipitation_hours = values("precipitation_hours")
    total_precipitation_hours = sum(precipitation_hours) if precipitation_hours else None
    daypart_total_precipitation = 0.0
    daypart_total_precipitation_hours = 0.0
    for row in rows:
        row_dayparts = row.get("dayparts") if isinstance(row.get("dayparts"), dict) else {}
        for item in row_dayparts.values():
            if not isinstance(item, dict):
                continue
            if isinstance(item.get("precipitation_mm"), (int, float)):
                daypart_total_precipitation += float(item["precipitation_mm"])
            if isinstance(item.get("precipitation_hours"), (int, float)):
                daypart_total_precipitation_hours += float(item["precipitation_hours"])
    precipitation_share_denominator = (
        daypart_total_precipitation if daypart_total_precipitation > 0 else total_precipitation
    )
    precipitation_hours_share_denominator = (
        daypart_total_precipitation_hours if daypart_total_precipitation_hours > 0 else total_precipitation_hours
    )
    daypart_summary = {}
    for daypart, _, _ in HISTORICAL_DAYPARTS:
        part_rows = [
            row.get("dayparts", {}).get(daypart, {})
            for row in rows
            if isinstance(row.get("dayparts"), dict)
        ]
        part_precipitation = [
            float(item["precipitation_mm"])
            for item in part_rows
            if isinstance(item.get("precipitation_mm"), (int, float))
        ]
        part_rain = [
            float(item["rain_mm"])
            for item in part_rows
            if isinstance(item.get("rain_mm"), (int, float))
        ]
        part_snowfall = [
            float(item["snowfall_cm"])
            for item in part_rows
            if isinstance(item.get("snowfall_cm"), (int, float))
        ]
        part_hours = [
            float(item["precipitation_hours"])
            for item in part_rows
            if isinstance(item.get("precipitation_hours"), (int, float))
        ]
        part_available = [
            float(item["hours_available"])
            for item in part_rows
            if isinstance(item.get("hours_available"), (int, float))
        ]
        part_total = sum(part_precipitation)
        part_hour_total = sum(part_hours)
        daypart_summary[daypart] = {
            "precipitation_total_mm": round(part_total, 3) if part_precipitation else None,
            "rain_total_mm": round(sum(part_rain), 3) if part_rain else None,
            "snowfall_total_cm": round(sum(part_snowfall), 3) if part_snowfall else None,
            "precipitation_hours": round(part_hour_total, 3) if part_hours else None,
            "hours_available": round(sum(part_available), 3) if part_available else None,
            "days_with_precipitation": sum(value > 0 for value in part_precipitation) if part_precipitation else None,
            "days_with_data": sum(value > 0 for value in part_available) if part_available else None,
            "share_of_precipitation_mm": (
                round(part_total / precipitation_share_denominator, 3)
                if precipitation_share_denominator not in (None, 0)
                else None
            ),
            "share_of_precipitation_hours": (
                round(part_hour_total / precipitation_hours_share_denominator, 3)
                if precipitation_hours_share_denominator not in (None, 0)
                else None
            ),
        }

    return {
        "days": len(rows),
        "days_with_precipitation_data": len(precipitation),
        "temperature_mean_c": round(mean(temperatures), 3) if temperatures else None,
        "temperature_min_c": round(min(temperature_mins), 3) if temperature_mins else None,
        "temperature_max_c": round(max(temperature_maxes), 3) if temperature_maxes else None,
        "cloud_cover_mean_pct": round(mean(cloud), 3) if cloud else None,
        "precipitation_total_mm": round(sum(precipitation), 3) if precipitation else None,
        "precipitation_mean_mm": round(mean(precipitation), 3) if precipitation else None,
        "precipitation_median_mm": round(median(precipitation), 3) if precipitation else None,
        "precipitation_hours_total": round(sum(values("precipitation_hours")), 3) if values("precipitation_hours") else None,
        "rain_total_mm": round(sum(values("rain_mm")), 3) if values("rain_mm") else None,
        "snowfall_total_cm": round(sum(values("snowfall_cm")), 3) if values("snowfall_cm") else None,
        "precipitation_days_gt_0_5mm": count_above(0.5),
        "precipitation_days_gt_2mm": count_above(2),
        "precipitation_days_gt_5mm": count_above(5),
        "precipitation_day_fraction_gt_0_5mm": fraction_above(0.5),
        "precipitation_day_fraction_gt_2mm": fraction_above(2),
        "precipitation_day_fraction_gt_5mm": fraction_above(5),
        "daypart_summary": daypart_summary,
    }


def run_historical_comparison(config: dict, client: ApiClient, generated_at: str, data_date: str) -> dict:
    years, window_days = historical_policy(config)
    group_windows, global_windows = historical_windows(config)
    point_ids = list(active_points(config))
    result_groups = copy.deepcopy(group_windows)
    requests = {}
    failures = {}

    for year in years:
        year_key = str(year)
        global_window = global_windows[year_key]
        params = historical_request_params(config, global_window["start"], global_window["end"])
        try:
            payloads, url = client.get_json(
                OPEN_METEO_ENDPOINTS["history"],
                params,
                f"historical:{year}",
                allow_array=True,
            )
            if not isinstance(payloads, list) or len(payloads) != len(point_ids):
                raise OpenMeteoError("HISTORICAL_POINT_COUNT_MISMATCH")
            point_payloads = dict(zip(point_ids, payloads))
            point_rows = {}
            for point_id in point_ids:
                payload = point_payloads[point_id]
                if not isinstance(payload, dict):
                    raise OpenMeteoError(f"HISTORICAL_POINT_NOT_OBJECT:{point_id}")
                point_rows[point_id] = historical_daily_rows(payload)
            requests[year_key] = {
                "url": url,
                "parameters": params,
                "requested_point_ids": point_ids,
                "returned_points": [
                    {
                        "point_id": point_id,
                        "latitude": point_payloads[point_id].get("latitude"),
                        "longitude": point_payloads[point_id].get("longitude"),
                        "elevation": point_payloads[point_id].get("elevation"),
                        "timezone": point_payloads[point_id].get("timezone"),
                    }
                    for point_id in point_ids
                ],
            }
            for group_id, group in result_groups.items():
                group_window = group["by_year"][year_key]
                start = group_window["start"]
                end = group_window["end"]
                group_points = {}
                for point_id in group["point_ids"]:
                    rows = [row for row in point_rows[point_id] if start <= row["date"] <= end]
                    rows_by_date = {row["date"]: row for row in rows}
                    payload = point_payloads[point_id]
                    group_points[point_id] = {
                        "point": config["points"][point_id],
                        "response": {**response_meta(payload), "endpoint_url": url},
                        "target_dates": {
                            target_date: rows_by_date.get(target_date)
                            for target_date in group_window["target_dates"]
                        },
                        "daily": rows,
                        "window_summary": historical_window_summary(rows),
                    }
                group["by_year"][year_key] = {
                    **group_window,
                    "status": "OK",
                    "points": group_points,
                }
        except Exception as error:
            reason = f"{type(error).__name__}:{error}"
            failures[year_key] = reason
            for group in result_groups.values():
                group["by_year"][year_key] = {
                    **group["by_year"][year_key],
                    "status": "FAILED",
                    "error": reason,
                }

    status = "OK" if len(requests) == len(years) else "PARTIAL" if requests else "FAILED"
    return module_header(
        "historical_comparison",
        generated_at,
        data_date,
        status,
        source="Open-Meteo Historical Weather API",
        endpoint=OPEN_METEO_ENDPOINTS["history"],
        years=years,
        window_days_each_side=window_days,
        query_windows=global_windows,
        target_groups=result_groups,
        requests=requests,
        failures=failures,
        interpretation_boundary=(
            "历史值来自Open-Meteo再分析格点，用于季节和近日期对照，"
            "不是景区内气象站实测；不同海拔和沟段存在局地差异。"
        ),
    )


# ---------------------------------------------------------------------------
# Target day summary
# ---------------------------------------------------------------------------
# A point record carries its member statistics under different keys depending on
# which module produced it: the ensemble module nests them under
# ``ensemble.distributions``, while the GEFS segment publishes plain
# ``daily[].statistics``.  They are normalised here into one per-date row shape
# so the target summary reads a single vocabulary.
TARGET_ENSEMBLE_VARIABLE_MAP = {
    "temperature_mean_c": "temperature_2m",
    "temperature_min_c": "temperature_2m_min",
    "temperature_max_c": "temperature_2m_max",
    "precipitation_mm": "precipitation",
    "snowfall_cm": "snowfall",
    "cloud_cover_mean_pct": "cloud_cover",
    "cloud_cover_low_mean_pct": "cloud_cover_low",
    "cloud_cover_high_mean_pct": "cloud_cover_high",
    "relative_humidity_mean_pct": "relative_humidity_2m",
    "wind_gust_max_kmh": "wind_gusts_10m",
}
TARGET_ENSEMBLE_NIGHT_MIN_FALLBACK = "temperature_2m_min"


def _target_statistics_rows(rows: object, statistics_key: str) -> dict[str, dict]:
    """Index a ``{date, <statistics_key>: {...}}`` list by date."""
    indexed: dict[str, dict] = {}
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        date = row.get("date")
        statistics = row.get(statistics_key)
        if isinstance(date, str) and isinstance(statistics, dict):
            indexed[date] = statistics
    return indexed


def target_ensemble_daily_rows(record: dict | None) -> list[dict]:
    """Return per-date member distributions for a point record.

    Records that already carry ``ensemble_daily`` rows are passed through
    unchanged so older archive snapshots keep working; otherwise the rows are
    rebuilt from whichever distribution layout the record provides.
    """
    if not isinstance(record, dict):
        return []
    legacy = record.get("ensemble_daily")
    if isinstance(legacy, list) and legacy:
        return legacy

    statistics_by_date: dict[str, dict] = {}
    night_min_by_date: dict[str, dict] = {}
    probabilities_by_date: dict[str, dict] = {}

    ensemble = record.get("ensemble")
    if isinstance(ensemble, dict) and isinstance(ensemble.get("distributions"), dict):
        distributions = ensemble["distributions"]
        statistics_by_date = _target_statistics_rows(distributions.get("variables"), "statistics")
        night_min_by_date = _target_statistics_rows(distributions.get("night_min"), "statistics_c")
        probabilities_by_date = _target_statistics_rows(distributions.get("probabilities"), "probabilities")
    else:
        statistics_by_date = _target_statistics_rows(record.get("daily"), "statistics")

    rows = []
    for date in sorted(statistics_by_date):
        statistics = statistics_by_date[date]
        row: dict = {"date": date}
        for output_key, source_key in TARGET_ENSEMBLE_VARIABLE_MAP.items():
            value = statistics.get(source_key)
            row[output_key] = value if isinstance(value, dict) else None
        night_min = night_min_by_date.get(date)
        if isinstance(night_min, dict):
            row["night_min_c"] = night_min
        else:
            fallback = statistics.get(TARGET_ENSEMBLE_NIGHT_MIN_FALLBACK)
            row["night_min_c"] = fallback if isinstance(fallback, dict) else None
        probabilities = probabilities_by_date.get(date)
        if not isinstance(probabilities, dict):
            inline = statistics.get("probabilities")
            probabilities = inline if isinstance(inline, dict) else None
        row["probabilities"] = probabilities
        rows.append(row)
    return rows


def find_daily(record: dict | None, target_date: str) -> dict | None:
    if not isinstance(record, dict):
        return None
    return next((item for item in record.get("daily", []) if item.get("date") == target_date), None)


def find_ensemble_daily(record: dict | None, target_date: str) -> dict | None:
    if not isinstance(record, dict):
        return None
    return next((item for item in target_ensemble_daily_rows(record) if item.get("date") == target_date), None)


def geffs_record(module: dict, point_id: str, segment: str) -> dict | None:
    """Return the segment payload for a GEFS point.

    Segments live directly under ``points[point_id][segment]``; a nested
    ``record`` wrapper from older snapshots is still accepted.
    """
    if not isinstance(module, dict):
        return None
    point = (module.get("points") or {}).get(point_id)
    if not isinstance(point, dict):
        return None
    segment_value = point.get(segment)
    if not isinstance(segment_value, dict):
        return None
    nested = segment_value.get("record")
    return nested if isinstance(nested, dict) else segment_value


def target_coverage_level(hres: dict | None, gfs: dict | None, ecmwf: dict | None, gefs: dict | None) -> str:
    if hres:
        return "HRES_AVAILABLE"
    if gfs:
        return "GFS_AVAILABLE"
    if ecmwf:
        return "ECMWF_ENSEMBLE_AVAILABLE"
    if gefs:
        return "GEFS_LONG_RANGE_AVAILABLE"
    return "NOT_YET_AVAILABLE"


def deterministic_consensus(hres: dict | None, gfs: dict | None, gefs: dict | None) -> dict:
    sources = [("hres", hres), ("gfs", gfs)]
    values = {}
    for output_key, input_key in (
        ("temperature_mean_c", "temperature_mean_c"),
        ("temperature_min_c", "temperature_min_c"),
        ("temperature_max_c", "temperature_max_c"),
        ("night_min_c", "night_min_c"),
        ("precipitation_mm", "precipitation_mm"),
        ("snowfall_cm", "snowfall_cm"),
        ("cloud_cover_mean_pct", "cloud_cover_mean_pct"),
        ("cloud_cover_low_mean_pct", "cloud_cover_low_mean_pct"),
    ):
        candidates = [item[input_key] for _, item in sources if item and isinstance(item.get(input_key), (int, float))]
        if gefs and isinstance(gefs.get(input_key), dict) and isinstance(gefs[input_key].get("mean"), (int, float)):
            candidates.append(gefs[input_key]["mean"])
        values[output_key] = round(mean(candidates), 3) if candidates else None
    return {"values": values, "sources": [name for name, item in sources if item] + (["gefs_long_range"] if gefs else [])}


def build_target_summary(config: dict, generated_at: str, data_date: str, modules: dict) -> dict:
    groups = {}
    target_dates = []
    for group_id, group in config["target_groups"].items():
        target_dates.extend(group["dates"])
        by_date = {}
        for target_date in group["dates"]:
            points = {}
            for point_id in group["point_ids"]:
                hres = find_daily(modules["hres"].get("points", {}).get(point_id), target_date)
                gfs = find_daily(modules["gfs"].get("points", {}).get(point_id), target_date)
                ecmwf = find_ensemble_daily(modules["ensemble"].get("points", {}).get(point_id), target_date)
                gefs = find_ensemble_daily(geffs_record(modules["gefs"], point_id, "long_range"), target_date)
                level = target_coverage_level(hres, gfs, ecmwf, gefs)
                if level == "NOT_YET_AVAILABLE":
                    confidence = "not_available"
                elif level == "HRES_AVAILABLE":
                    confidence = "near_term"
                elif level == "GFS_AVAILABLE":
                    confidence = "medium_term"
                elif level == "ECMWF_ENSEMBLE_AVAILABLE":
                    confidence = "ensemble_range"
                else:
                    confidence = "long_range_trend"
                points[point_id] = {
                    "point": config["points"][point_id],
                    "lead_days": (dt.date.fromisoformat(target_date) - dt.date.fromisoformat(data_date)).days,
                    "coverage_status": level,
                    "confidence_class": confidence,
                    "sources_available": {
                        "hres": bool(hres),
                        "gfs": bool(gfs),
                        "ecmwf_ensemble": bool(ecmwf),
                        "gefs_long_range": bool(gefs),
                    },
                    "metrics": {
                        "hres": hres,
                        "gfs": gfs,
                        "ecmwf_ensemble": ecmwf,
                        "gefs_long_range": gefs,
                        "consensus": deterministic_consensus(hres, gfs, gefs),
                    },
                    "interpretation": {
                        "precipitation_signal": (gefs or ecmwf or {}).get("probabilities", {}) if (gefs or ecmwf) else {},
                        "cloud_signal": (gefs or ecmwf or {}).get("cloud_cover_mean_pct", {}) if (gefs or ecmwf) else {},
                        "temperature_signal": (gefs or ecmwf or {}).get("temperature_mean_c", {}) if (gefs or ecmwf) else {},
                    },
                }
            statuses = [item["coverage_status"] for item in points.values()]
            unique_statuses = set(statuses)
            if len(unique_statuses) == 1:
                day_coverage_status = next(iter(unique_statuses))
            elif any(value != "NOT_YET_AVAILABLE" for value in statuses):
                day_coverage_status = "PARTIAL"
            else:
                day_coverage_status = "NOT_YET_AVAILABLE"
            by_date[target_date] = {
                "coverage_status": day_coverage_status,
                "points": points,
            }
        groups[group_id] = {"name": group["name"], "dates": group["dates"], "point_ids": group["point_ids"], "by_date": by_date}
    return {
        "schema_version": SCHEMA_VERSION,
        "module": "target_summary",
        "status": "OK",
        "generated_at": generated_at,
        "data_date": data_date,
        "target_dates": sorted(set(target_dates)),
        "target_groups": groups,
        "interpretation_boundary": (
            "温度、降水、云量均为对应模型返回格点的预报聚合；"
            "四姑娘山高位路段和九寨沟沟内不同海拔会产生局地差异。"
        ),
    }


def build_summary(
    config: dict,
    generated_at: str,
    data_date: str,
    now_local: dt.datetime,
    hres: dict,
    history: dict,
    ensemble: dict,
    gfs: dict,
    single_runs: dict,
    spatial: dict,
    long_range: dict | None = None,
    weather_events: dict | None = None,
    gefs: dict | None = None,
) -> dict:
    active = active_points(config)
    history_regions = (history.get("region_summaries") or {}).get("regions", {})
    long_range_regions = (long_range or {}).get("regions") or {}
    regions = {}
    for region_id, region_config in config["regions"].items():
        core_id = region_config.get("core_point_id")
        core_point = active.get(core_id) if core_id else None
        hres_record = (hres.get("points") or {}).get(core_id) if core_id else None
        gfs_record = (gfs.get("points") or {}).get(core_id) if core_id else None
        ensemble_record = (ensemble.get("points") or {}).get(core_id) if core_id else None
        history_region = history_regions.get(region_id)
        single_region = (single_runs.get("regions") or {}).get(region_id)
        spatial_region = (spatial.get("regions") or {}).get(region_id)
        long_range_region = long_range_regions.get(region_id)
        weather_events_region = ((weather_events or {}).get("regions") or {}).get(region_id)
        if not core_point:
            regions[region_id] = {
                "visit_date": region_config.get("primary_visit_date"),
                "usable_for_main_chain": False,
                "weather_driver_vs_2025": undetermined_weather_driver("NO_VERIFIED_CORE_POINT"),
                **{
                    f"weather_driver_vs_{year}": undetermined_weather_driver("NO_VERIFIED_CORE_POINT")
                    for year in history_years_for_config(config)
                    if year not in (2025, 2026)
                },
                "forecast_0_7d": {"status": "UNAVAILABLE", "reason": "NO_VERIFIED_CORE_POINT"},
                "forecast_8_15d": {"status": "UNAVAILABLE", "reason": "NO_VERIFIED_CORE_POINT"},
                "ensemble": {"status": "UNAVAILABLE", "reason": "NO_VERIFIED_CORE_POINT"},
                "gfs_crosscheck": {"status": "UNAVAILABLE", "reason": "NO_VERIFIED_CORE_POINT"},
                "leaf_loss_weather_risk": {"status": "UNAVAILABLE", "reason": "NO_VERIFIED_CORE_POINT"},
                "forecast_16_35d": unavailable_long_range_summary("NO_VERIFIED_CORE_POINT"),
                "qa": summary_qa(None, None, None, None, None, spatial_region, long_range_region, weather_events_region),
            }
            if weather_events is not None:
                regions[region_id]["weather_events"] = weather_event_light_summary(weather_events, region_id)
            continue
        hres_days = (hres_record or {}).get("daily", [])
        forecast_short = forecast_0_7d(hres_days) if hres_record and hres_record.get("status") == "PASS" else {"status": "UNAVAILABLE", "reason": "HRES_INVALID"}
        forecast_long = forecast_8_15d(hres_days) if hres_record and hres_record.get("status") == "PASS" else {"status": "UNAVAILABLE", "reason": "HRES_INVALID"}
        ensemble_summary = {
            "status": ensemble_record.get("status") if ensemble_record else "FAILED",
            "model": ensemble.get("model"),
            "model_id": ensemble.get("model_id"),
            "total_members": ensemble.get("total_members"),
            "distribution": (ensemble_record.get("ensemble") or {}).get("distributions") if ensemble_record else None,
            "qa": ensemble_record.get("qa") if ensemble_record else None,
        }
        regions[region_id] = {
            "visit_date": region_config.get("primary_visit_date"),
            "usable_for_main_chain": True,
            "core_point_id": core_id,
            "weather_driver_vs_2025": (history_region or {}).get("weather_driver_vs_2025") or undetermined_weather_driver("HISTORY_INVALID"),
            **{
                f"weather_driver_vs_{year}": (history_region or {}).get(f"weather_driver_vs_{year}") or undetermined_weather_driver("HISTORY_INVALID")
                for year in history_years_for_config(config)
                if year not in (2025, 2026)
            },
            "forecast_0_7d": forecast_short,
            "forecast_8_15d": forecast_long,
            "ensemble": ensemble_summary,
            "gfs_crosscheck": gfs_crosscheck(hres_record, gfs_record),
            "leaf_loss_weather_risk": leaf_loss_weather_risk(hres_days, now_local.date()) if hres_record and hres_record.get("status") == "PASS" else {"status": "UNAVAILABLE", "reason": "HRES_INVALID"},
            "forecast_16_35d": long_range_summary_for_chatgpt(long_range_region),
            "qa": summary_qa(hres_record, history_region, ensemble_record, gfs_record, single_region, spatial_region, long_range_region, weather_events_region),
        }
        if weather_events is not None:
            regions[region_id]["weather_events"] = weather_event_light_summary(weather_events, region_id)
    target_window_brief = (
        build_target_window_brief(
            config,
            now_local.date(),
            hres,
            gfs,
            ensemble,
            gefs,
            GEFS_TRAVEL_CUTOFF_DATE,
            generated_at,
        )
        if gefs is not None
        else {
            "status": "UNAVAILABLE",
            "reason": "GEFS_MODULE_UNAVAILABLE",
            "cutoff_date": GEFS_TRAVEL_CUTOFF_DATE.isoformat(),
        }
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "data_date": data_date,
        "forecast_date": now_local.date().isoformat(),
        "completed_history_date": data_date,
        "history_years": list(history_years_for_config(config)),
        "visit_dates": {
            region_id: config["regions"][region_id].get("visit_dates", [])
            for region_id in config["regions"]
        },
        "phenology_weather_summary_path": "data/latest/phenology_weather_summary.json",
        "weather_events_path": "data/latest/weather_events.json",
        "gefs_path": "data/latest/gefs.json",
        "target_window_brief": target_window_brief,
        "regions": regions,
        "manual_phenology_baseline": config.get("manual_phenology_baseline"),
        "interpretation_boundary": "This file reports weather drivers and weather event risk. It does not produce a final autumn-colour or phenology conclusion.",
    }


def failed_module(
    name: str,
    generated_at: str,
    data_date: str,
    error: Exception,
    *,
    artifact_module: str | None = None,
) -> dict:
    reason = f"{type(error).__name__}:{error}"
    log(f"[{name}] MODULE FAILED: {reason}")
    return module_header(artifact_module or name, generated_at, data_date, "FAILED", error=reason, points={}, regions={})


def compact_record(record: dict) -> dict:
    keep = (
        "point_id",
        "point",
        "status",
        "source",
        "endpoint",
        "model",
        "request",
        "response",
        "qa",
        "solar_variable",
        "daily",
        "ensemble",
        "error",
        "variable_status",
        "requested_variables",
        "required_variables",
        "optional_variables",
        "degraded_variables",
        "required_unavailable_variables",
        "optional_unavailable_variables",
        "unavailable_variables",
    )
    return {key: copy.deepcopy(record[key]) for key in keep if key in record}


def compact_module(name: str, value: dict) -> dict:
    compact = copy.deepcopy(value)
    compact["archive_kind"] = "compact_daily_snapshot"
    compact["hourly_values_omitted"] = True
    if name in {"hres", "gfs"}:
        compact["points"] = {point_id: compact_record(record) for point_id, record in value.get("points", {}).items()}
    elif name == "history_comparison":
        compact["points"] = {}
        for point_id, point_result in value.get("points", {}).items():
            item = copy.deepcopy(point_result)
            item["years"] = {year: compact_record(record) for year, record in point_result.get("years", {}).items()}
            compact["points"][point_id] = item
        compact["region_summaries"] = copy.deepcopy(value.get("region_summaries", {}))
    elif name == "history_forward":
        compact["points"] = {}
        for point_id, point_result in value.get("points", {}).items():
            item = copy.deepcopy(point_result)
            item["years"] = {
                year: compact_history_forward_year(record)
                for year, record in point_result.get("years", {}).items()
            }
            compact["points"][point_id] = item
        compact["regions"] = copy.deepcopy(value.get("regions", {}))
    elif name == "ensemble":
        compact["points"] = {}
        for point_id, record in value.get("points", {}).items():
            item = compact_record(record)
            if "ensemble" in record:
                item["ensemble"] = copy.deepcopy(record["ensemble"])
            compact["points"][point_id] = item
    elif name == "single_runs":
        compact["regions"] = {}
        for region_id, region in value.get("regions", {}).items():
            item = copy.deepcopy(region)
            item["runs"] = []
            for run in region.get("runs", []):
                run_item = {key: copy.deepcopy(run[key]) for key in ("init_time", "status", "target_time", "target") if key in run}
                if "record" in run:
                    run_item["record"] = compact_record(run["record"])
                item["runs"].append(run_item)
            compact["regions"][region_id] = item
    elif name == "long_range":
        compact.pop("raw_points", None)
        compact.pop("raw_references", None)
        compact["raw_hourly_included"] = False
    elif name == "gefs":
        compact.pop("raw_points", None)
        compact["raw_hourly_included"] = False
    elif name == "weather_events":
        compact["raw_hourly_included"] = False
    elif name == "historical_comparison":
        # The archive snapshot keeps the request provenance and the per-window
        # aggregates; the per-day rows are the bulk of the artifact and are
        # exactly what ``compact_daily_snapshot`` is meant to drop.
        compact["requests"] = {
            year: {
                key: copy.deepcopy(item[key])
                for key in ("url", "requested_point_ids", "returned_points")
                if key in item
            }
            for year, item in value.get("requests", {}).items()
        }
        for group in compact.get("target_groups", {}).values():
            for year_item in group.get("by_year", {}).values():
                for point_result in (year_item.get("points") or {}).values():
                    point_result.pop("daily", None)
    return compact


def public_long_range_artifact(value: dict) -> dict:
    """Remove member-level hourly payloads from the machine-readable artifact."""
    public = copy.deepcopy(value)
    public.pop("raw_points", None)
    public.pop("raw_references", None)
    public["raw_hourly_included"] = False
    public["raw_snapshot_retention_days"] = RAW_RETENTION_DAYS
    return public


def public_gefs_artifact(value: dict) -> dict:
    """Keep member distributions/phases while omitting full member hourly arrays."""
    public = copy.deepcopy(value)
    public.pop("raw_points", None)
    public["raw_hourly_included"] = False
    public["raw_snapshot_retention_days"] = RAW_RETENTION_DAYS
    return public


def prune_old_raw_archives(current_archive_date: dt.date) -> None:
    cutoff = current_archive_date - dt.timedelta(days=RAW_RETENTION_DAYS - 1)
    if not ARCHIVE_DIR.exists():
        return
    for child in ARCHIVE_DIR.iterdir():
        if not child.is_dir():
            continue
        try:
            archive_date = dt.date.fromisoformat(child.name)
        except ValueError:
            continue
        if archive_date < cutoff:
            raw_dir = child / "raw"
            if raw_dir.is_dir():
                # Retention is intentionally limited to generated raw snapshots only.
                shutil.rmtree(raw_dir)


def write_outputs(
    *,
    now_local: dt.datetime,
    status: dict,
    hres: dict,
    history: dict,
    history_forward: dict,
    ensemble: dict,
    gfs: dict,
    single_runs: dict,
    spatial: dict,
    long_range: dict,
    summary: dict,
    grid_registry: dict | None = None,
    phenology_weather_summary: dict | None = None,
    weather_events: dict | None = None,
    gefs: dict | None = None,
    target_summary: dict | None = None,
    historical_comparison: dict | None = None,
) -> None:
    grid_registry = grid_registry or {}
    phenology_weather_summary = phenology_weather_summary or {}
    weather_events = weather_events or {}
    gefs = gefs or {}
    target_summary = target_summary or {}
    historical_comparison = historical_comparison or {}
    artifacts = {
        "status.json": status,
        "hres.json": hres,
        "history_comparison.json": history,
        "history_forward.json": history_forward,
        "ensemble.json": ensemble,
        "gfs.json": gfs,
        "single_runs.json": single_runs,
        "spatial_sampling.json": spatial,
        "long_range.json": public_long_range_artifact(long_range),
        "grid_registry.json": grid_registry,
        "phenology_weather_summary.json": phenology_weather_summary,
        "weather_events.json": weather_events,
        "gefs.json": public_gefs_artifact(gefs),
        "summary.json": summary,
        "target_summary.json": target_summary,
        "historical_comparison.json": historical_comparison,
    }
    for filename, artifact in artifacts.items():
        writer = write_compact_json if filename == "phenology_weather_summary.json" else write_json
        writer(LATEST_DIR / filename, artifact)
    archive_path = ARCHIVE_DIR / now_local.date().isoformat()
    archive_path.mkdir(parents=True, exist_ok=True)
    for filename, artifact in artifacts.items():
        name = filename.removesuffix(".json")
        archive_artifact = compact_module(name, artifact)
        writer = write_compact_json if filename == "phenology_weather_summary.json" else write_json
        writer(archive_path / filename, archive_artifact)
    raw_values = {
        "hres.json.gz": hres,
        "history_comparison.json.gz": history,
        "history_forward.json.gz": history_forward,
        "ensemble.json.gz": ensemble,
        "gfs.json.gz": gfs,
        "single_runs.json.gz": single_runs,
        "spatial_sampling.json.gz": spatial,
        "long_range.json.gz": long_range,
        "grid_registry.json.gz": grid_registry,
        "weather_events.json.gz": weather_events,
        "gefs.json.gz": gefs,
        "target_summary.json.gz": target_summary,
        "historical_comparison.json.gz": historical_comparison,
    }
    for filename, artifact in raw_values.items():
        write_gzip_json(archive_path / "raw" / filename, artifact)
    prune_old_raw_archives(now_local.date())


def build_status(
    config: dict,
    generated_at: str,
    data_date: str,
    modules: dict[str, dict],
    target_summary: dict | None = None,
) -> dict:
    module_names = ("hres", "history", "ensemble", "gfs", "single_runs")
    module_values = {name: modules.get(name, {}).get("status", "FAILED") for name in module_names}
    history_forward_status = modules.get("history_forward", {}).get("status", "FAILED")
    long_range_status = modules.get("long_range", {}).get("status", "FAILED")
    light_summary_status = modules.get("phenology_weather_summary", {}).get("status")
    weather_events_status = modules.get("weather_events", {}).get("status")
    gefs_status = modules.get("gefs", {}).get("status", "SKIPPED")
    # The two trip-facing artifacts are supplementary to the forecast modules,
    # but a failure there still means the published picture is incomplete, so
    # they downgrade the run instead of passing silently.
    target_summary_status = modules.get("target_summary", {}).get("status")
    historical_comparison_status = modules.get("historical_comparison", {}).get("status")
    if all(value == "OK" for value in module_values.values()):
        pipeline_status = (
            "OK"
            if long_range_status == "OK"
            and history_forward_status == "OK"
            and light_summary_status in {None, "OK"}
            and weather_events_status in {None, "OK"}
            and gefs_status in {None, "OK", "SKIPPED"}
            and target_summary_status in {None, "OK"}
            and historical_comparison_status in {None, "OK"}
            else "PARTIAL"
        )
    elif modules.get("hres", {}).get("status") == "OK" or modules.get("history", {}).get("status") == "OK":
        pipeline_status = "DEGRADED"
    else:
        pipeline_status = "FAILED"
    points = {}
    for point_id, point in config.get("points", {}).items():
        verified = point.get("status") == "VERIFIED"
        points[point_id] = {
            "name": point.get("name"),
            "region": point.get("region"),
            "status": point.get("status"),
            "usable_for_main_chain": verified,
            "reason": None if verified else point.get("reason") or "PROVISIONAL_POINT_EXCLUDED",
        }
    module_details = {
        name: {
            "status": value.get("status", "FAILED"),
            "successful_points": value.get("successful_points", value.get("successful_fetches")),
            "partial_points": value.get("partial_points"),
            "failed_points": value.get("failed_points", value.get("failed_fetches")),
            "error": value.get("error"),
        }
        for name, value in modules.items()
    }
    # Each model family publishes its own per-variable availability.  A variable
    # that is not served by a model is reported here explicitly instead of being
    # silently dropped or filled from another model.
    model_variable_status = {
        "ecmwf_deterministic_hres": modules.get("hres", {}).get("variable_status", {}),
        "ecmwf_ensemble": modules.get("ensemble", {}).get("variable_status", {}),
        "gfs_deterministic": modules.get("gfs", {}).get("variable_status", {}),
        "gefs_ensemble": modules.get("gefs", {}).get("variable_status", {}),
    }
    for name, module_name in (
        ("hres", "ecmwf_deterministic_hres"),
        ("gfs", "gfs_deterministic"),
        ("ensemble", "ecmwf_ensemble"),
        ("gefs", "gefs_ensemble"),
    ):
        if name not in module_details:
            continue
        value = modules.get(name, {})
        module_details[name]["variable_status"] = value.get("variable_status", {})
        module_details[name]["required_unavailable_variables"] = value.get("required_unavailable_variables", [])
        module_details[name]["optional_unavailable_variables"] = value.get("optional_unavailable_variables", [])
        module_details[name]["model_key"] = module_name
    gefs_module = modules.get("gefs") or {}
    if gefs_module:
        module_details.setdefault("gefs", {}).update({
            "usable_points": gefs_module.get("usable_points"),
            "required_missing_variables": gefs_module.get("required_missing_variables", []),
            "optional_missing_variables": gefs_module.get("optional_missing_variables", []),
            "cloud_layer_status": gefs_module.get("cloud_layer_status", {}),
            "warnings": gefs_module.get("qa_warnings", []),
            "point_status_summary": gefs_module.get("point_status_summary", {}),
        })
    result = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "data_date": data_date,
        "history_years": list(history_years_for_config(config)),
        "pipeline_status": pipeline_status,
        "modules": module_values | {
            "spatial_sampling": modules.get("spatial_sampling", {}).get("status", "FAILED"),
            "long_range": long_range_status,
            "history_forward": history_forward_status,
            "gefs": gefs_status,
        },
        "module_details": module_details,
        "points": points,
        "route_slots": {
            slot_id: {
                "status": slot.get("status"),
                "enabled": slot.get("enabled", False),
                "usable_for_main_chain": False,
                "reason": slot.get("reason") or "ROUTE_NOT_VERIFIED",
            }
            for slot_id, slot in config.get("route_slots", {}).items()
        },
        "manual_phenology_baseline": config.get("manual_phenology_baseline"),
        "variable_status": model_variable_status,
        "variable_model_policy": {
            "models": [
                "ECMWF deterministic / HRES",
                "ECMWF ensemble",
                "GFS deterministic",
                "GEFS ensemble",
            ],
            "independent": True,
            "cross_model_averaging": False,
            "unavailable_values_are_null": True,
            "note": "An OPTIONAL_UNAVAILABLE variable is missing at the source; it is never substituted from another model.",
        },
        "failure_policy": "Any request, QA, model, timezone, missing-data, or grid-representativeness failure is recorded as INVALID; no external weather fallback is used.",
    }
    if light_summary_status is not None:
        result["modules"]["phenology_weather_summary"] = light_summary_status
    if weather_events_status is not None:
        result["modules"]["weather_events"] = weather_events_status
    if target_summary_status is not None:
        result["modules"]["target_summary"] = target_summary_status
    if historical_comparison_status is not None:
        result["modules"]["historical_comparison"] = historical_comparison_status
    if target_summary is not None:
        result["target_coverage"] = {
            group_id: {
                target_date: {
                    "coverage_status": day["coverage_status"],
                    "points_available": sum(
                        any(entry["sources_available"].values()) for entry in day["points"].values()
                    ),
                    "points_total": len(day["points"]),
                    "target_date_in_model_window": day["coverage_status"] != "NOT_YET_AVAILABLE",
                }
                for target_date, day in group.get("by_date", {}).items()
            }
            for group_id, group in target_summary.get("target_groups", {}).items()
        }
        result["target_dates"] = sorted(
            {date for group in config.get("target_groups", {}).values() for date in group.get("dates", [])}
        )
    return result


def minimal_failure_status(generated_at: str, reason: str) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "data_date": None,
        "pipeline_status": "FAILED",
        "modules": {"hres": "FAILED", "history": "FAILED", "history_forward": "FAILED", "ensemble": "FAILED", "gfs": "FAILED", "single_runs": "FAILED", "spatial_sampling": "FAILED", "long_range": "FAILED", "gefs": "FAILED", "phenology_weather_summary": "FAILED", "weather_events": "FAILED"},
        "module_details": {"pipeline": {"status": "FAILED", "error": reason}},
        "points": {},
        "route_slots": {},
        "failure_policy": "Pipeline initialization failed before data retrieval.",
    }


def run_pipeline(
    now_utc: dt.datetime | None = None,
    *,
    refresh_history: bool = False,
) -> dict:
    now_utc = (now_utc or dt.datetime.now(UTC)).astimezone(UTC)
    now_local = now_utc.astimezone(LOCAL_TZ)
    generated_at = iso_utc(now_utc)
    data_date = (now_local.date() - dt.timedelta(days=1)).isoformat()
    config = load_config()
    client = ApiClient()
    modules: dict[str, dict] = {}

    log("PHASE 1: HRES")
    try:
        modules["hres"] = run_hres(config, client, generated_at, data_date)
    except Exception as error:
        modules["hres"] = failed_module("hres", generated_at, data_date, error)

    log("PHASE 2: HISTORICAL IFS")
    try:
        modules["history"] = run_history(
            config,
            client,
            generated_at,
            data_date,
            now_local.date() - dt.timedelta(days=1),
            refresh_history=refresh_history,
            forward_anchor_date=now_local.date(),
        )
    except Exception as error:
        modules["history"] = failed_module("history", generated_at, data_date, error)

    log("PHASE 3: HISTORICAL FORWARD PATH")
    try:
        modules["history_forward"] = run_history_forward(
            config,
            client,
            generated_at,
            data_date,
            now_local.date(),
            # run_history above refreshes the union needed by the forward
            # paths, so reusing the cache here avoids a second API request in
            # the same pipeline run.
            refresh_history=False,
        )
    except Exception as error:
        modules["history_forward"] = failed_history_forward_module(
            config,
            generated_at,
            data_date,
            now_local.date(),
            error,
        )

    log("PHASE 4: SPATIAL SAMPLING")
    try:
        modules["spatial_sampling"] = run_spatial(config, client, generated_at, data_date, modules["hres"])
    except Exception as error:
        modules["spatial_sampling"] = failed_module("spatial_sampling", generated_at, data_date, error)

    log("PHASE 5: GFS")
    try:
        modules["gfs"] = run_gfs(config, client, generated_at, data_date)
    except Exception as error:
        modules["gfs"] = failed_module("gfs", generated_at, data_date, error)

    log("PHASE 6: ECMWF ENSEMBLE")
    try:
        modules["ensemble"] = run_ensemble(config, client, generated_at, data_date)
    except Exception as error:
        modules["ensemble"] = failed_module("ensemble", generated_at, data_date, error)

    log("PHASE 7: SINGLE RUNS")
    try:
        modules["single_runs"] = run_single_runs(config, client, generated_at, data_date, now_utc)
    except Exception as error:
        modules["single_runs"] = failed_module("single_runs", generated_at, data_date, error)

    log("PHASE 8: GFS ENSEMBLE LONG RANGE")
    try:
        modules["long_range"] = run_long_range(
            config,
            client,
            generated_at,
            data_date,
            now_local,
            # Historical IFS and forward-path phases already warm the cache;
            # the reference layer only fills dates outside that union.
            refresh_history=False,
        )
    except Exception as error:
        modules["long_range"] = failed_module(
            "long_range",
            generated_at,
            data_date,
            error,
            artifact_module="long_range_background",
        )

    log("PHASE 8A: WEATHER EVENTS")
    try:
        modules["weather_events"] = run_weather_events(
            config,
            modules["hres"],
            generated_at,
            data_date,
            now_local.date(),
        )
    except Exception as error:
        modules["weather_events"] = failed_module(
            "weather_events",
            generated_at,
            data_date,
            error,
        )

    log("PHASE 8B: INDEPENDENT GEFS")
    try:
        modules["gefs"] = run_gefs(
            config,
            client,
            generated_at,
            data_date,
            cutoff_date=GEFS_TRAVEL_CUTOFF_DATE,
        )
    except Exception as error:
        modules["gefs"] = failed_module("gefs", generated_at, data_date, error)

    log("PHASE 8C: THREE-YEAR HISTORICAL COMPARISON")
    try:
        modules["historical_comparison"] = run_historical_comparison(config, client, generated_at, data_date)
    except Exception as error:
        modules["historical_comparison"] = failed_module("historical_comparison", generated_at, data_date, error)

    log("PHASE 9: SUMMARY")
    try:
        summary = build_summary(
            config,
            generated_at,
            data_date,
            now_local,
            modules["hres"],
            modules["history"],
            modules["ensemble"],
            modules["gfs"],
            modules["single_runs"],
            modules["spatial_sampling"],
            modules["long_range"],
            modules["weather_events"],
            modules["gefs"],
        )
    except Exception as error:
        log(f"[summary] BUILD FAILED: {type(error).__name__}:{error}")
        summary = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": generated_at,
            "data_date": data_date,
            "phenology_weather_summary_path": "data/latest/phenology_weather_summary.json",
            "regions": {},
            "error": f"{type(error).__name__}:{error}",
            "interpretation_boundary": "Summary unavailable; inspect status.json and module artifacts.",
        }
    modules["summary"] = {"status": "OK" if "error" not in summary else "FAILED"}
    log("PHASE 9A: GRID REGISTRY")
    try:
        grid_registry = build_grid_registry(
            config,
            modules["hres"],
            modules["history_forward"],
            generated_at,
            data_date,
        )
    except Exception as error:
        log(f"[grid_registry] BUILD FAILED: {type(error).__name__}:{error}")
        grid_registry = {
            "schema_version": SCHEMA_VERSION,
            "module": "grid_registry",
            "generated_at": generated_at,
            "data_date": data_date,
            "status": "FAILED",
            "error": f"{type(error).__name__}:{error}",
        }
    log("PHASE 9B: LIGHTWEIGHT WEATHER SUMMARY")
    try:
        phenology_weather_summary = build_phenology_weather_summary(
            config,
            generated_at,
            data_date,
            now_local.date(),
            modules["hres"],
            modules["history_forward"],
            modules["weather_events"],
        )
        modules["phenology_weather_summary"] = {
            "status": "OK",
            "successful_regions": sum(
                value.get("status") == "OK"
                for value in phenology_weather_summary.get("regions", {}).values()
            ),
            "failed_points": 0,
            "error": None,
        }
    except Exception as error:
        log(f"[phenology_weather_summary] BUILD FAILED: {type(error).__name__}:{error}")
        phenology_weather_summary = {
            "schema_version": SCHEMA_VERSION,
            "module": "phenology_weather_summary",
            "generated_at": generated_at,
            "data_date": data_date,
            "forecast_date": now_local.date().isoformat(),
            "source": "Open-Meteo",
            "weather_only": True,
            "status": "FAILED",
            "error": f"{type(error).__name__}:{error}",
            "regions": {},
        }
        modules["phenology_weather_summary"] = {
            "status": "FAILED",
            "successful_regions": 0,
            "failed_points": 0,
            "error": f"{type(error).__name__}:{error}",
        }
    log("PHASE 10: TARGET DAY SUMMARY")
    try:
        target_summary = build_target_summary(config, generated_at, data_date, modules)
    except Exception as error:
        log(f"[target_summary] BUILD FAILED: {type(error).__name__}:{error}")
        target_summary = {
            "schema_version": SCHEMA_VERSION,
            "module": "target_summary",
            "status": "FAILED",
            "generated_at": generated_at,
            "data_date": data_date,
            "target_dates": [],
            "target_groups": {},
            "error": f"{type(error).__name__}:{error}",
        }
    modules["target_summary"] = {"status": target_summary.get("status", "FAILED")}
    status = build_status(config, generated_at, data_date, modules, target_summary)
    write_outputs(
        now_local=now_local,
        status=status,
        hres=modules["hres"],
        history=modules["history"],
        history_forward=modules["history_forward"],
        ensemble=modules["ensemble"],
        gfs=modules["gfs"],
        single_runs=modules["single_runs"],
        spatial=modules["spatial_sampling"],
        long_range=modules["long_range"],
        summary=summary,
        grid_registry=grid_registry,
        phenology_weather_summary=phenology_weather_summary,
        weather_events=modules["weather_events"],
        gefs=modules["gefs"],
        target_summary=target_summary,
        historical_comparison=modules["historical_comparison"],
    )
    log(f"PIPELINE STATUS: {status['pipeline_status']}")
    for name, value in status["modules"].items():
        log(f"MODULE {name}: {value}")
    return status


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--now", help="override current time with an ISO-8601 timestamp for reproducible runs")
    parser.add_argument(
        "--refresh-history",
        action="store_true",
        help="revalidate historical cache ranges against Open-Meteo before rebuilding outputs",
    )
    args = parser.parse_args(argv)
    refresh_history = args.refresh_history or os.environ.get("SIGUNIANG_JIUZHAIGOU_MONITOR_REFRESH_HISTORY", "").lower() in {
        "1",
        "true",
        "yes",
    }
    try:
        run_pipeline(now_from_input(args.now), refresh_history=refresh_history)
        return 0
    except Exception as error:
        reason = f"{type(error).__name__}:{error}"
        generated_at = iso_utc(dt.datetime.now(UTC))
        log(f"PIPELINE INITIALIZATION FAILED: {reason}")
        try:
            write_json(LATEST_DIR / "status.json", minimal_failure_status(generated_at, reason))
        except Exception as write_error:
            log(f"STATUS WRITE FAILED: {type(write_error).__name__}:{write_error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
