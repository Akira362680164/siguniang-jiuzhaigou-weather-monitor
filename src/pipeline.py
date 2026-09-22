#!/usr/bin/env python3
"""Open-Meteo weather evidence pipeline for the Four Girls / Jiuzhaigou tracker."""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import gzip
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
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

try:
    import certifi
except ImportError:  # pragma: no cover - CI installs requirements.txt
    certifi = None


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "points.json"
LATEST_DIR = ROOT / "data" / "latest"
ARCHIVE_DIR = ROOT / "data" / "archive"
TIMEZONE_NAME = "Asia/Shanghai"
LOCAL_TZ = ZoneInfo(TIMEZONE_NAME)
UTC = dt.timezone.utc
SCHEMA_VERSION = "1.0.0"

ENDPOINTS = {
    "hres": "https://api.open-meteo.com/v1/ecmwf",
    "gfs": "https://api.open-meteo.com/v1/gfs",
    "ensemble": "https://ensemble-api.open-meteo.com/v1/ensemble",
}
ALLOWED_HOSTS = {"api.open-meteo.com", "ensemble-api.open-meteo.com"}

STANDARD_VARIABLES = [
    "temperature_2m",
    "precipitation",
    "snowfall",
    "cloud_cover",
    "cloud_cover_low",
    "wind_speed_10m",
    "wind_gusts_10m",
    "relative_humidity_2m",
]
ENSEMBLE_VARIABLES = [
    "temperature_2m",
    "precipitation",
    "snowfall",
    "cloud_cover",
    "cloud_cover_low",
    "wind_gusts_10m",
]
GEFS_REQUIRED_VARIABLES = ["temperature_2m", "precipitation", "snowfall", "cloud_cover"]
GEFS_OPTIONAL_VARIABLES = ["cloud_cover_low", "wind_gusts_10m"]
MODEL_SPECS = {
    "hres": {
        "label": "ECMWF IFS HRES 9 km",
        "model_id": "ecmwf_ifs",
        "forecast_days": 15,
        "grid_limit_km": 16.0,
        "precision_module": "hres",
    },
    "gfs": {
        "label": "NOAA GFS Global 0.11°",
        "model_id": "gfs_global",
        "forecast_days": 16,
        "grid_limit_km": 20.0,
        "precision_module": "gfs",
    },
    "ensemble": {
        "label": "ECMWF IFS 0.25° Ensemble",
        "model_id": "ecmwf_ifs025_ensemble",
        "forecast_days": 7,
        "grid_limit_km": 38.0,
        "temporal_resolution": "hourly_3",
    },
}
GEFS_SPECS = {
    "near_range": {
        "label": "NOAA GEFS 0.25°",
        "model_id": "ncep_gefs025",
        "forecast_days": 10,
        "grid_limit_km": 30.0,
        "resolution": "0.25° (~25 km)",
        "temporal_resolution": "hourly_3",
    },
    "long_range": {
        "label": "NOAA GEFS 0.5°",
        "model_id": "ncep_gefs05",
        "forecast_days": 35,
        "grid_limit_km": 45.0,
        "resolution": "0.5° (~50 km)",
        "temporal_resolution": "hourly_3",
    },
}


class OpenMeteoError(RuntimeError):
    """A request failed against an allow-listed Open-Meteo endpoint."""

    def __init__(self, reason: str, *, status_code: int | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.status_code = status_code


def log(message: str) -> None:
    print(message, flush=True)


def now_from_input(value: str | None = None) -> dt.datetime:
    raw = value or os.environ.get("SIGUNIANG_MONITOR_NOW")
    if raw:
        parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return dt.datetime.now(UTC)


def iso_utc(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_local_api_time(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value)
    return parsed.replace(tzinfo=LOCAL_TZ)


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


def write_gzip_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    with gzip.open(temp_path, "wt", encoding="utf-8", compresslevel=9) as handle:
        json.dump(value, handle, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    temp_path.replace(path)


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


def load_config(path: Path = CONFIG_PATH) -> dict:
    with path.open(encoding="utf-8") as handle:
        config = json.load(handle)
    if config.get("timezone") != TIMEZONE_NAME:
        raise ValueError(f"config timezone must be {TIMEZONE_NAME}")
    if config.get("namespace") != "siguniang_jiuzhaigou":
        raise ValueError("unexpected config namespace")
    if not isinstance(config.get("points"), dict) or not config["points"]:
        raise ValueError("points must be a non-empty object")
    for point_id, point in config["points"].items():
        if point.get("status") != "VERIFIED":
            raise ValueError(f"point {point_id} must be VERIFIED for this tracker")
        if not valid_coordinate(point.get("latitude"), point.get("longitude")):
            raise ValueError(f"point {point_id} has invalid coordinates")
    target_dates = set()
    for group_id, group in config.get("target_groups", {}).items():
        if not group.get("dates") or not group.get("point_ids"):
            raise ValueError(f"target group {group_id} needs dates and point_ids")
        for raw_date in group["dates"]:
            dt.date.fromisoformat(raw_date)
            target_dates.add(raw_date)
        missing = set(group["point_ids"]) - set(config["points"])
        if missing:
            raise ValueError(f"target group {group_id} references missing points: {sorted(missing)}")
    if not target_dates:
        raise ValueError("target_groups must contain at least one target date")
    return config


def active_points(config: dict) -> dict[str, dict]:
    result = {}
    for point_id, raw_point in config["points"].items():
        point = dict(raw_point)
        point["id"] = point_id
        if point.get("status") == "VERIFIED":
            result[point_id] = point
    return result


def excluded_points(config: dict) -> dict[str, dict]:
    return {
        point_id: {"name": point.get("name"), "status": point.get("status"), "reason": "not VERIFIED"}
        for point_id, point in config["points"].items()
        if point.get("status") != "VERIFIED"
    }


class ApiClient:
    def __init__(self, timeout: int = 60, retries: int = 3) -> None:
        self.timeout = timeout
        self.retries = retries
        self.ssl_context = ssl.create_default_context(cafile=certifi.where()) if certifi else ssl.create_default_context()

    def get_json(self, endpoint: str, params: dict[str, object], label: str) -> tuple[dict, str]:
        host = urlparse(endpoint).hostname
        if host not in ALLOWED_HOSTS:
            raise OpenMeteoError(f"HOST_NOT_ALLOWLISTED:{host}")
        query = urlencode({key: str(value) for key, value in params.items()})
        url = f"{endpoint}?{query}"
        for attempt in range(1, self.retries + 1):
            try:
                request = Request(url, headers={"User-Agent": "siguniang-jiuzhaigou-weather-monitor/1.0"})
                with urlopen(request, timeout=self.timeout, context=self.ssl_context) as response:
                    body = response.read().decode("utf-8")
                payload = json.loads(body)
                if not isinstance(payload, dict):
                    raise OpenMeteoError("RESPONSE_NOT_OBJECT")
                return payload, url
            except HTTPError as error:
                if error.code in {429, 500, 502, 503, 504} and attempt < self.retries:
                    delay = min(8, 2 ** (attempt - 1))
                    log(f"[{label}] HTTP {error.code}; retrying in {delay}s")
                    time.sleep(delay)
                    continue
                raise OpenMeteoError(f"HTTP_{error.code}", status_code=error.code) from error
            except (URLError, TimeoutError, json.JSONDecodeError) as error:
                if attempt < self.retries:
                    delay = min(8, 2 ** (attempt - 1))
                    log(f"[{label}] {type(error).__name__}; retrying in {delay}s")
                    time.sleep(delay)
                    continue
                raise OpenMeteoError(f"{type(error).__name__}:{error}") from error


def base_params(point: dict, **extra: object) -> dict[str, object]:
    return {
        "latitude": point["latitude"],
        "longitude": point["longitude"],
        "timezone": TIMEZONE_NAME,
        "cell_selection": "nearest",
        "elevation": "nan",
        **extra,
    }


def values_at(hourly: dict, key: str, indices: list[int]) -> list[float]:
    values = hourly.get(key)
    if not isinstance(values, list):
        return []
    result = []
    for index in indices:
        if index < len(values) and values[index] is not None:
            result.append(float(values[index]))
    return result


def percentile(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return round(ordered[lower], 3)
    fraction = position - lower
    return round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction, 3)


def statistics(values: list[float]) -> dict:
    p10 = percentile(values, 0.10)
    p25 = percentile(values, 0.25)
    p75 = percentile(values, 0.75)
    p90 = percentile(values, 0.90)
    return {
        "mean": round(mean(values), 3) if values else None,
        "median": round(median(values), 3) if values else None,
        "p10": p10,
        "p25": p25,
        "p75": p75,
        "p90": p90,
        "spread_p90_p10": round(p90 - p10, 3) if p10 is not None and p90 is not None else None,
        "interquartile_spread": round(p75 - p25, 3) if p25 is not None and p75 is not None else None,
        "members_with_data": len(values),
    }


def precision_class(lead_hours: float, module: str) -> str:
    if module == "gfs":
        if lead_hours < 120:
            return "native_hourly"
        return "coarse_3h_interpolated"
    if lead_hours < 90:
        return "native_hourly"
    if lead_hours < 144:
        return "coarse_3h_interpolated"
    return "trend_only_6h_plus"


def daily_metrics(hourly: dict, module: str | None = None) -> list[dict]:
    times = hourly.get("time") if isinstance(hourly.get("time"), list) else []
    if not times:
        return []
    try:
        parsed = [parse_local_api_time(value) for value in times]
    except (TypeError, ValueError):
        return []
    first_time = parsed[0]
    groups: dict[str, list[int]] = {}
    for index, local_time in enumerate(parsed):
        groups.setdefault(local_time.date().isoformat(), []).append(index)
    result = []
    for day, indices in sorted(groups.items()):
        temperatures = values_at(hourly, "temperature_2m", indices)
        night_indices = [index for index in indices if parsed[index].hour <= 6 or parsed[index].hour >= 20]
        night_temperatures = values_at(hourly, "temperature_2m", night_indices)
        classes = [
            precision_class((parsed[index] - first_time).total_seconds() / 3600, module)
            for index in indices
        ] if module else []
        result.append({
            "date": day,
            "hours_available": len(indices),
            "complete": len(indices) >= 20 and len(temperatures) == len(indices),
            "temperature_min_c": round(min(temperatures), 3) if temperatures else None,
            "temperature_max_c": round(max(temperatures), 3) if temperatures else None,
            "temperature_mean_c": round(mean(temperatures), 3) if temperatures else None,
            "night_min_c": round(min(night_temperatures), 3) if night_temperatures else None,
            "precipitation_mm": round(sum(values_at(hourly, "precipitation", indices)), 3),
            "snowfall_cm": round(sum(values_at(hourly, "snowfall", indices)), 3),
            "cloud_cover_mean_pct": round(mean(values_at(hourly, "cloud_cover", indices)), 3) if values_at(hourly, "cloud_cover", indices) else None,
            "cloud_cover_low_mean_pct": round(mean(values_at(hourly, "cloud_cover_low", indices)), 3) if values_at(hourly, "cloud_cover_low", indices) else None,
            "wind_speed_mean_kmh": round(mean(values_at(hourly, "wind_speed_10m", indices)), 3) if values_at(hourly, "wind_speed_10m", indices) else None,
            "wind_gust_max_kmh": round(max(values_at(hourly, "wind_gusts_10m", indices)), 3) if values_at(hourly, "wind_gusts_10m", indices) else None,
            "relative_humidity_mean_pct": round(mean(values_at(hourly, "relative_humidity_2m", indices)), 3) if values_at(hourly, "relative_humidity_2m", indices) else None,
            "precision_class": ",".join(dict.fromkeys(classes)) if classes else None,
        })
    return result


def response_meta(payload: dict, url: str) -> dict:
    return {
        "grid_coordinate": {"latitude": payload.get("latitude"), "longitude": payload.get("longitude")},
        "returned_elevation": payload.get("elevation"),
        "timezone": payload.get("timezone"),
        "utc_offset_seconds": payload.get("utc_offset_seconds"),
        "returned_model": payload.get("model"),
        "returned_model_id": payload.get("model_id"),
        "model_run_initialization": payload.get("model_run_initialization"),
        "generationtime_ms": payload.get("generationtime_ms"),
        "retrieval_time": iso_utc(dt.datetime.now(UTC)),
        "endpoint_url": url,
    }


def trim_incomplete_edge_rows(payload: dict, required_variables: list[str]) -> tuple[dict, dict]:
    """Remove API edge rows where a required series has not initialized/ended."""
    output = copy.deepcopy(payload)
    hourly = output.get("hourly") if isinstance(output.get("hourly"), dict) else {}
    times = hourly.get("time") if isinstance(hourly.get("time"), list) else []
    original_count = len(times)
    leading = 0
    trailing = 0

    def missing_at(index: int) -> bool:
        for variable in required_variables:
            values = hourly.get(variable)
            if not isinstance(values, list) or index >= len(values) or values[index] is None:
                return True
        return False

    while times and missing_at(0):
        for values in hourly.values():
            if isinstance(values, list) and values:
                values.pop(0)
        leading += 1
        times = hourly.get("time") if isinstance(hourly.get("time"), list) else []
    while times and missing_at(len(times) - 1):
        for values in hourly.values():
            if isinstance(values, list) and values:
                values.pop()
        trailing += 1
        times = hourly.get("time") if isinstance(hourly.get("time"), list) else []
    output["hourly"] = hourly
    return output, {
        "original_timestep_count": original_count,
        "retained_timestep_count": len(times),
        "leading_missing_rows_removed": leading,
        "trailing_missing_rows_removed": trailing,
        "edge_trim_status": "TRIMMED" if leading or trailing else "COMPLETE",
    }


def validate_payload(
    payload: dict,
    point: dict,
    requested_variables: list[str],
    required_variables: list[str],
    grid_limit_km: float,
) -> dict:
    hourly = payload.get("hourly") if isinstance(payload.get("hourly"), dict) else {}
    times = hourly.get("time") if isinstance(hourly.get("time"), list) else []
    grid = {"latitude": payload.get("latitude"), "longitude": payload.get("longitude")}
    coordinate_ok = valid_coordinate(grid["latitude"], grid["longitude"])
    grid_distance = haversine_km(point["latitude"], point["longitude"], grid["latitude"], grid["longitude"]) if coordinate_ok else None
    distance_ok = grid_distance is not None and grid_distance <= grid_limit_km
    timezone_ok = payload.get("timezone") == TIMEZONE_NAME and payload.get("utc_offset_seconds") == 28800
    missing = [name for name in requested_variables if name not in hourly]
    length_mismatch = [name for name in requested_variables if isinstance(hourly.get(name), list) and len(hourly[name]) != len(times)]
    required_missing = [name for name in required_variables if name not in hourly]
    required_null = [name for name in required_variables if name in hourly and any(value is None for value in hourly[name])]
    optional_missing = [name for name in requested_variables if name not in required_variables and name not in hourly]
    optional_null = [name for name in requested_variables if name not in required_variables and name in hourly and any(value is None for value in hourly[name])]
    core_valid = bool(times) and coordinate_ok and distance_ok and timezone_ok and not required_missing and not required_null and not length_mismatch
    status = "PASS" if core_valid and not optional_missing and not optional_null else "PARTIAL" if core_valid else "INVALID"
    reasons = []
    if not times:
        reasons.append("NO_HOURLY_DATA")
    if not coordinate_ok:
        reasons.append("RETURNED_COORDINATE_INVALID")
    if not distance_ok:
        reasons.append("GRID_DISTANCE_OVER_LIMIT")
    if not timezone_ok:
        reasons.append("TIMEZONE_MISMATCH")
    if required_missing:
        reasons.append("REQUIRED_VARIABLE_MISSING:" + ",".join(required_missing))
    if required_null:
        reasons.append("REQUIRED_VARIABLE_NULL:" + ",".join(required_null))
    if optional_missing:
        reasons.append("OPTIONAL_VARIABLE_MISSING:" + ",".join(optional_missing))
    if optional_null:
        reasons.append("OPTIONAL_VARIABLE_NULL:" + ",".join(optional_null))
    return {
        "valid": core_valid,
        "final_status": status,
        "grid_distance_km": round(grid_distance, 3) if grid_distance is not None else None,
        "grid_distance_limit_km": grid_limit_km,
        "requested_coordinate": {"latitude": point["latitude"], "longitude": point["longitude"]},
        "returned_coordinate": grid,
        "timezone_check": "PASS" if timezone_ok else "FAIL",
        "coordinate_check": "PASS" if coordinate_ok else "FAIL",
        "distance_check": "PASS" if distance_ok else "FAIL",
        "missing_variables": missing,
        "array_length_mismatch": length_mismatch,
        "required_null_variables": required_null,
        "optional_missing_variables": optional_missing,
        "optional_null_variables": optional_null,
        "reason": ";".join(reasons) or None,
    }


def invalid_record(point: dict, endpoint: str, model: str, params: dict, reason: str, error: OpenMeteoError | None = None) -> dict:
    return {
        "point_id": point["id"],
        "point": {key: point.get(key) for key in ("name", "region", "role", "status", "latitude", "longitude")},
        "status": "INVALID",
        "source": "Open-Meteo",
        "endpoint": endpoint,
        "model": model,
        "request": {"coordinate": {"latitude": point["latitude"], "longitude": point["longitude"]}, "parameters": params},
        "response": None,
        "qa": {"valid": False, "final_status": "INVALID", "reason": reason},
        "error": {"reason": reason, "http_status": error.status_code if error else None},
    }


def fetch_point(
    client: ApiClient,
    point: dict,
    *,
    endpoint: str,
    model: str,
    model_id: str,
    variables: list[str],
    required_variables: list[str],
    forecast_days: int,
    grid_limit_km: float,
    module: str,
    extra_params: dict[str, object] | None = None,
) -> dict:
    params = base_params(point, forecast_days=forecast_days, **(extra_params or {}))
    request_params = {**params, "hourly": ",".join(variables)}
    label = f"{point['id']}:{module}"
    try:
        payload, url = client.get_json(endpoint, request_params, label)
    except OpenMeteoError as error:
        log(f"[{label}] FETCH FAILED: {error.reason}")
        return invalid_record(point, endpoint, model, request_params, "OPEN_METEO_REQUEST_FAILED:" + error.reason, error)
    trim_variables = required_variables if module.startswith("gefs_") else variables
    payload, edge_trim = trim_incomplete_edge_rows(payload, trim_variables)
    qa = validate_payload(payload, point, variables, required_variables, grid_limit_km)
    qa["edge_trim"] = edge_trim
    record = {
        "point_id": point["id"],
        "point": {key: point.get(key) for key in ("name", "region", "role", "status", "latitude", "longitude", "coordinate_note")},
        "status": qa["final_status"],
        "source": "Open-Meteo",
        "endpoint": endpoint,
        "model": model,
        "model_id": model_id,
        "request": {"coordinate": {"latitude": point["latitude"], "longitude": point["longitude"]}, "parameters": request_params},
        "response": response_meta(payload, url),
        "qa": qa,
    }
    if qa["valid"]:
        hourly = payload.get("hourly") if isinstance(payload.get("hourly"), dict) else {}
        record["hourly"] = hourly
        record["daily"] = daily_metrics(hourly, module if module in {"hres", "gfs"} else None)
        log(f"[{label}] {qa['final_status']} grid={qa.get('grid_distance_km')}km")
    return record


def module_status(records: list[dict]) -> str:
    if not records or not any(record.get("status") in {"PASS", "PARTIAL"} for record in records):
        return "FAILED"
    if all(record.get("status") == "PASS" for record in records):
        return "OK"
    return "PARTIAL"


def module_header(name: str, generated_at: str, data_date: str, status: str, **extra: object) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "module": name,
        "status": status,
        "generated_at": generated_at,
        "data_date": data_date,
        **extra,
    }


def failed_module(name: str, generated_at: str, data_date: str, error: Exception) -> dict:
    return module_header(name, generated_at, data_date, "FAILED", error=f"{type(error).__name__}:{error}", points={})


def run_standard_module(config: dict, client: ApiClient, generated_at: str, data_date: str, module: str) -> dict:
    spec = MODEL_SPECS[module]
    records = {}
    for point_id, point in active_points(config).items():
        records[point_id] = fetch_point(
            client,
            point,
            endpoint=ENDPOINTS[module],
            model=spec["label"],
            model_id=spec["model_id"],
            variables=STANDARD_VARIABLES,
            required_variables=["temperature_2m"],
            forecast_days=spec["forecast_days"],
            grid_limit_km=spec["grid_limit_km"],
            module=module,
        )
    values = list(records.values())
    return module_header(
        module,
        generated_at,
        data_date,
        module_status(values),
        source="Open-Meteo",
        endpoint=ENDPOINTS[module],
        model=spec["label"],
        model_id=spec["model_id"],
        forecast_days=spec["forecast_days"],
        points=records,
        excluded_points=excluded_points(config),
        successful_points=sum(record.get("status") == "PASS" for record in values),
        partial_points=sum(record.get("status") == "PARTIAL" for record in values),
        failed_points=sum(record.get("status") == "INVALID" for record in values),
        interpretation_boundary="返回格点和模型分辨率会限制景区内部精度；逐日值是预报聚合，不是现场实况。",
    )


def member_keys(hourly: dict, variable: str) -> list[str]:
    keys = [variable] if isinstance(hourly.get(variable), list) else []
    pattern = re.compile(re.escape(variable) + r"_member\d{2}$")
    keys.extend(sorted((key for key in hourly if pattern.fullmatch(key)), key=lambda key: int(key.rsplit("member", 1)[1])))
    return keys


def member_statistics(values: list[float], expected_members: int) -> dict:
    result = statistics(values)
    result["expected_members"] = expected_members
    return result


def member_daily_distributions(hourly: dict, expected_members: int) -> list[dict]:
    times = hourly.get("time") if isinstance(hourly.get("time"), list) else []
    if not times:
        return []
    parsed = [parse_local_api_time(value) for value in times]
    groups: dict[str, list[int]] = {}
    for index, local_time in enumerate(parsed):
        groups.setdefault(local_time.date().isoformat(), []).append(index)
    temp_keys = member_keys(hourly, "temperature_2m")
    output = []
    for day, indices in sorted(groups.items()):
        night_indices = [index for index in indices if parsed[index].hour <= 6 or parsed[index].hour >= 20]
        per_member = []
        for key in temp_keys:
            temperatures = values_at(hourly, key, indices)
            night_temperatures = values_at(hourly, key, night_indices)
            precipitation = values_at(
                hourly, key.replace("temperature_2m", "precipitation"), indices
            )
            snowfall = values_at(
                hourly, key.replace("temperature_2m", "snowfall"), indices
            )
            cloud = values_at(hourly, key.replace("temperature_2m", "cloud_cover"), indices)
            low_cloud = values_at(hourly, key.replace("temperature_2m", "cloud_cover_low"), indices)
            gusts = values_at(hourly, key.replace("temperature_2m", "wind_gusts_10m"), indices)
            if not temperatures:
                continue
            per_member.append({
                "temperature_mean_c": round(mean(temperatures), 3),
                "temperature_min_c": round(min(temperatures), 3),
                "temperature_max_c": round(max(temperatures), 3),
                "night_min_c": round(min(night_temperatures), 3) if night_temperatures else None,
                "precipitation_mm": round(sum(precipitation), 3) if precipitation else None,
                "snowfall_cm": round(sum(snowfall), 3) if snowfall else None,
                "cloud_cover_mean_pct": round(mean(cloud), 3) if cloud else None,
                "cloud_cover_low_mean_pct": round(mean(low_cloud), 3) if low_cloud else None,
                "wind_gust_max_kmh": round(max(gusts), 3) if gusts else None,
            })

        def metric(name: str) -> list[float]:
            return [float(item[name]) for item in per_member if item.get(name) is not None]

        precipitation = metric("precipitation_mm")
        snowfall = metric("snowfall_cm")
        cloud = metric("cloud_cover_mean_pct")
        low_cloud = metric("cloud_cover_low_mean_pct")
        night_min = metric("night_min_c")
        gust = metric("wind_gust_max_kmh")
        temp_min = metric("temperature_min_c")
        output.append({
            "date": day,
            "members_valid": len(per_member),
            "temperature_mean_c": member_statistics(metric("temperature_mean_c"), expected_members),
            "temperature_min_c": member_statistics(temp_min, expected_members),
            "temperature_max_c": member_statistics(metric("temperature_max_c"), expected_members),
            "night_min_c": member_statistics(night_min, expected_members),
            "precipitation_mm": member_statistics(precipitation, expected_members),
            "snowfall_cm": member_statistics(snowfall, expected_members),
            "cloud_cover_mean_pct": member_statistics(cloud, expected_members),
            "cloud_cover_low_mean_pct": member_statistics(low_cloud, expected_members),
            "wind_gust_max_kmh": member_statistics(gust, expected_members),
            "probabilities": {
                "precipitation_gt_0_5mm": round(sum(value > 0.5 for value in precipitation) / len(precipitation), 3) if precipitation else None,
                "precipitation_gt_2mm": round(sum(value > 2 for value in precipitation) / len(precipitation), 3) if precipitation else None,
                "precipitation_gt_5mm": round(sum(value > 5 for value in precipitation) / len(precipitation), 3) if precipitation else None,
                "snowfall_gt_0_5cm": round(sum(value > 0.5 for value in snowfall) / len(snowfall), 3) if snowfall else None,
                "cloud_cover_gt_70pct": round(sum(value > 70 for value in cloud) / len(cloud), 3) if cloud else None,
                "cloud_cover_low_gt_50pct": round(sum(value > 50 for value in low_cloud) / len(low_cloud), 3) if low_cloud else None,
                "temperature_min_lt_0c": round(sum(value < 0 for value in night_min) / len(night_min), 3) if night_min else None,
                "temperature_min_lt_minus5c": round(sum(value < -5 for value in night_min) / len(night_min), 3) if night_min else None,
                "gust_gt_40kmh": round(sum(value > 40 for value in gust) / len(gust), 3) if gust else None,
            },
        })
    return output


def validate_member_count(hourly: dict, variables: list[str], expected: int) -> dict:
    counts = {variable: len(member_keys(hourly, variable)) for variable in variables}
    temp_count = counts.get("temperature_2m", 0)
    status = "PASS" if temp_count == expected else "PARTIAL" if temp_count else "FAIL"
    return {
        "status": status,
        "expected_members": expected,
        "member_counts_by_variable": counts,
        "temperature_member_count": temp_count,
        "usable": temp_count > 0,
    }


def run_ensemble(config: dict, client: ApiClient, generated_at: str, data_date: str) -> dict:
    spec = MODEL_SPECS["ensemble"]
    records = {}
    for point_id, point in active_points(config).items():
        record = fetch_point(
            client,
            point,
            endpoint=ENDPOINTS["ensemble"],
            model=spec["label"],
            model_id=spec["model_id"],
            variables=ENSEMBLE_VARIABLES,
            required_variables=["temperature_2m"],
            forecast_days=spec["forecast_days"],
            grid_limit_km=spec["grid_limit_km"],
            module="ensemble",
            extra_params={
                "models": spec["model_id"],
                "temporal_resolution": spec["temporal_resolution"],
            },
        )
        if record.get("status") in {"PASS", "PARTIAL"}:
            check = validate_member_count(record.get("hourly", {}), ENSEMBLE_VARIABLES, 51)
            record["member_check"] = check
            record["ensemble_daily"] = member_daily_distributions(record["hourly"], 51)
            if not check["usable"]:
                record["status"] = "INVALID"
                record["qa"]["final_status"] = "INVALID"
                record["qa"]["reason"] = "NO_ENSEMBLE_MEMBER_DATA"
        records[point_id] = record
    values = list(records.values())
    return module_header(
        "ensemble",
        generated_at,
        data_date,
        module_status(values),
        source="Open-Meteo",
        endpoint=ENDPOINTS["ensemble"],
        model=spec["label"],
        model_id=spec["model_id"],
        resolution="0.25° (~25 km)",
        temporal_resolution=spec["temporal_resolution"],
        members_total=51,
        forecast_days=spec["forecast_days"],
        points=records,
        excluded_points=excluded_points(config),
        successful_points=sum(record.get("status") == "PASS" for record in values),
        partial_points=sum(record.get("status") == "PARTIAL" for record in values),
        failed_points=sum(record.get("status") == "INVALID" for record in values),
        interpretation_boundary="集合分布用于表达天气信号和不确定性，不等于景区内每个点的精确概率。",
    )


def run_gefs(config: dict, client: ApiClient, generated_at: str, data_date: str, raw_archive_path: str) -> dict:
    public_points = {}
    raw_points = {}
    for point_id, point in active_points(config).items():
        public_segments = {}
        raw_segments = {}
        for segment, spec in GEFS_SPECS.items():
            variables = [*GEFS_REQUIRED_VARIABLES, *GEFS_OPTIONAL_VARIABLES]
            record = fetch_point(
                client,
                point,
                endpoint=ENDPOINTS["ensemble"],
                model=spec["label"],
                model_id=spec["model_id"],
                variables=variables,
                required_variables=GEFS_REQUIRED_VARIABLES,
                forecast_days=spec["forecast_days"],
                grid_limit_km=spec["grid_limit_km"],
                module=f"gefs_{segment}",
                extra_params={
                    "models": spec["model_id"],
                    "temporal_resolution": spec["temporal_resolution"],
                },
            )
            raw_segments[segment] = record
            public_record = copy.deepcopy(record)
            if record.get("status") in {"PASS", "PARTIAL"}:
                member_check = validate_member_count(record.get("hourly", {}), variables, 31)
                record["member_check"] = member_check
                record["ensemble_daily"] = member_daily_distributions(record["hourly"], 31)
                public_record = copy.deepcopy(record)
                public_record.pop("hourly", None)
                public_record["raw_archive_path"] = raw_archive_path
                if not member_check["usable"]:
                    public_record["status"] = "INVALID"
                    public_record["qa"]["final_status"] = "INVALID"
                    public_record["qa"]["reason"] = "NO_GEFS_MEMBER_DATA"
            public_segments[segment] = {
                "status": public_record.get("status"),
                "model": spec["label"],
                "model_id": spec["model_id"],
                "resolution": spec["resolution"],
                "temporal_resolution": spec["temporal_resolution"],
                "forecast_days": spec["forecast_days"],
                "record": public_record,
            }
        segment_statuses = [item["record"].get("status") for item in public_segments.values()]
        point_status = "OK" if all(value == "PASS" for value in segment_statuses) else "PARTIAL" if any(value in {"PASS", "PARTIAL"} for value in segment_statuses) else "FAILED"
        public_points[point_id] = {
            "point_id": point_id,
            "point": {key: point.get(key) for key in ("name", "region", "role", "status", "latitude", "longitude", "coordinate_note")},
            "status": point_status,
            "near_range": public_segments["near_range"],
            "long_range": public_segments["long_range"],
        }
        raw_points[point_id] = raw_segments
    point_statuses = [item["status"] for item in public_points.values()]
    status = "OK" if point_statuses and all(value == "OK" for value in point_statuses) else "PARTIAL" if any(value in {"OK", "PARTIAL"} for value in point_statuses) else "FAILED"
    result = module_header(
        "gefs",
        generated_at,
        data_date,
        status,
        source="Open-Meteo",
        endpoint=ENDPOINTS["ensemble"],
        model="NOAA GEFS independent ensemble chain",
        members_total=31,
        near_range=GEFS_SPECS["near_range"],
        long_range=GEFS_SPECS["long_range"],
        points=public_points,
        excluded_points=excluded_points(config),
        raw_archive_path=raw_archive_path,
        raw_payload_in_latest=False,
        interpretation_boundary="GEFS 0.5°约50 km远期结果只用于趋势和不确定性提示；没有进入模型窗口的目标日不作推断。",
    )
    result["_raw_points"] = raw_points
    return result


def find_daily(record: dict | None, target_date: str) -> dict | None:
    if not isinstance(record, dict):
        return None
    return next((item for item in record.get("daily", []) if item.get("date") == target_date), None)


def find_ensemble_daily(record: dict | None, target_date: str) -> dict | None:
    if not isinstance(record, dict):
        return None
    return next((item for item in record.get("ensemble_daily", []) if item.get("date") == target_date), None)


def geffs_record(module: dict, point_id: str, segment: str) -> dict | None:
    point = module.get("points", {}).get(point_id, {})
    segment_value = point.get(segment, {})
    return segment_value.get("record") if isinstance(segment_value, dict) else None


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
        "interpretation_boundary": "温度、降水、云量均为对应模型返回格点的预报聚合；四姑娘山高位路段和九寨沟沟内不同海拔会产生局地差异。",
    }


def build_summary(target_summary: dict) -> dict:
    compact_groups = {}
    for group_id, group in target_summary["target_groups"].items():
        compact_groups[group_id] = {"name": group["name"], "dates": {}}
        for target_date, day in group["by_date"].items():
            point_entries = list(day["points"].values())
            available = [
                entry["metrics"]["consensus"]["values"]["temperature_mean_c"]
                for entry in point_entries
                if entry["metrics"]["consensus"]["values"].get("temperature_mean_c") is not None
            ]
            compact_groups[group_id]["dates"][target_date] = {
                "coverage_status": day["coverage_status"],
                "temperature_mean_range_c": [min(available), max(available)] if available else None,
                "points_available": sum(bool(entry["sources_available"]["hres"] or entry["sources_available"]["gfs"] or entry["sources_available"]["ecmwf_ensemble"] or entry["sources_available"]["gefs_long_range"]) for entry in point_entries),
                "points_total": len(point_entries),
            }
    return {
        "schema_version": SCHEMA_VERSION,
        "module": "summary",
        "status": "OK",
        "generated_at": target_summary["generated_at"],
        "data_date": target_summary["data_date"],
        "target_dates": target_summary["target_dates"],
        "target_groups": compact_groups,
        "artifacts": {
            "target_summary": "data/latest/target_summary.json",
            "hres": "data/latest/hres.json",
            "gfs": "data/latest/gfs.json",
            "ensemble": "data/latest/ensemble.json",
            "gefs": "data/latest/gefs.json",
        },
        "interpretation_boundary": target_summary["interpretation_boundary"],
    }


def build_status(config: dict, generated_at: str, data_date: str, modules: dict, target_summary: dict) -> dict:
    module_states = {name: value.get("status", "FAILED") for name, value in modules.items()}
    if any(value == "FAILED" for value in module_states.values()):
        pipeline_status = "PARTIAL" if any(value in {"OK", "PARTIAL"} for value in module_states.values()) else "FAILED"
    elif any(value == "PARTIAL" for value in module_states.values()):
        pipeline_status = "PARTIAL"
    else:
        pipeline_status = "OK"
    coverage = {}
    for group_id, group in target_summary["target_groups"].items():
        coverage[group_id] = {
            target_date: {
                "coverage_status": day["coverage_status"],
                "points_available": sum(
                    any(entry["sources_available"].values()) for entry in day["points"].values()
                ),
                "points_total": len(day["points"]),
                "target_date_in_model_window": day["coverage_status"] != "NOT_YET_AVAILABLE",
            }
            for target_date, day in group["by_date"].items()
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "module": "status",
        "generated_at": generated_at,
        "data_date": data_date,
        "pipeline_status": pipeline_status,
        "modules": module_states,
        "target_coverage": coverage,
        "target_dates": sorted({date for group in config["target_groups"].values() for date in group["dates"]}),
        "source": "Open-Meteo",
        "next_refresh_note": "GitHub Actions按北京时间02:17、08:17、14:17、20:17更新；模型窗口随日期推进，未覆盖目标日会自动进入远期/近期期待范围。",
    }


def write_outputs(now_local: dt.datetime, status: dict, summary: dict, target_summary: dict, modules: dict, raw_gefs: dict) -> None:
    public_gefs = copy.deepcopy(modules["gefs"])
    public_gefs.pop("_raw_points", None)
    artifacts = {
        "status.json": status,
        "summary.json": summary,
        "target_summary.json": target_summary,
        "hres.json": modules["hres"],
        "gfs.json": modules["gfs"],
        "ensemble.json": modules["ensemble"],
        "gefs.json": public_gefs,
    }
    for filename, value in artifacts.items():
        write_json(LATEST_DIR / filename, value)
    archive_day = ARCHIVE_DIR / now_local.date().isoformat()
    archive_day.mkdir(parents=True, exist_ok=True)
    for filename in artifacts:
        shutil.copy2(LATEST_DIR / filename, archive_day / filename)
    if raw_gefs:
        write_gzip_json(archive_day / "raw" / "gefs.json.gz", raw_gefs)


def run_pipeline(now_utc: dt.datetime) -> dict:
    config = load_config()
    now_local = now_utc.astimezone(LOCAL_TZ)
    generated_at = iso_utc(now_utc)
    data_date = now_local.date().isoformat()
    client = ApiClient()
    modules = {}
    for module in ("hres", "gfs"):
        try:
            modules[module] = run_standard_module(config, client, generated_at, data_date, module)
        except Exception as error:  # keep other model evidence publishable
            log(f"[{module}] MODULE FAILED: {type(error).__name__}:{error}")
            modules[module] = failed_module(module, generated_at, data_date, error)
    try:
        modules["ensemble"] = run_ensemble(config, client, generated_at, data_date)
    except Exception as error:
        log(f"[ensemble] MODULE FAILED: {type(error).__name__}:{error}")
        modules["ensemble"] = failed_module("ensemble", generated_at, data_date, error)
    raw_archive_path = f"data/archive/{data_date}/raw/gefs.json.gz"
    try:
        modules["gefs"] = run_gefs(config, client, generated_at, data_date, raw_archive_path)
    except Exception as error:
        log(f"[gefs] MODULE FAILED: {type(error).__name__}:{error}")
        modules["gefs"] = failed_module("gefs", generated_at, data_date, error)
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
    summary = build_summary(target_summary) if target_summary.get("status") == "OK" else {
        "schema_version": SCHEMA_VERSION,
        "module": "summary",
        "status": "FAILED",
        "generated_at": generated_at,
        "data_date": data_date,
        "target_dates": [],
        "target_groups": {},
        "error": target_summary.get("error"),
    }
    modules["target_summary"] = {"status": target_summary.get("status", "FAILED")}
    modules["summary"] = {"status": summary.get("status", "FAILED")}
    status = build_status(config, generated_at, data_date, modules, target_summary)
    raw_gefs = modules.get("gefs", {}).pop("_raw_points", {}) if isinstance(modules.get("gefs"), dict) else {}
    write_outputs(now_local, status, summary, target_summary, modules, raw_gefs)
    log(f"PIPELINE STATUS: {status['pipeline_status']}")
    for name, value in status["modules"].items():
        log(f"MODULE {name}: {value}")
    return status


def minimal_failure_status(reason: str) -> dict:
    generated_at = iso_utc(dt.datetime.now(UTC))
    return {
        "schema_version": SCHEMA_VERSION,
        "module": "status",
        "generated_at": generated_at,
        "data_date": generated_at[:10],
        "pipeline_status": "FAILED",
        "modules": {},
        "target_coverage": {},
        "error": reason,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--now", help="override artifact date with an ISO-8601 timestamp")
    args = parser.parse_args(argv)
    try:
        run_pipeline(now_from_input(args.now))
        return 0
    except Exception as error:
        reason = f"{type(error).__name__}:{error}"
        log(f"PIPELINE INITIALIZATION FAILED: {reason}")
        try:
            write_json(LATEST_DIR / "status.json", minimal_failure_status(reason))
        except Exception as write_error:
            log(f"STATUS WRITE FAILED: {type(write_error).__name__}:{write_error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
