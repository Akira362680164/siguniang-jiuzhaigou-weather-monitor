import sys
import copy
import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import pipeline


class PipelineUnitTests(unittest.TestCase):
    def make_payload(self, *, model="ecmwf_ifs", timezone="Asia/Shanghai", offset=28800):
        times = ["2026-09-01T00:00", "2026-09-01T01:00"]
        return {
            "latitude": 48.75,
            "longitude": 86.75,
            "elevation": 1664,
            "timezone": timezone,
            "utc_offset_seconds": offset,
            "model": model,
            "hourly": {
                "time": times,
                "temperature_2m": [1, 2],
                "precipitation": [0, 0],
            },
        }

    def make_day(self, day, night_min):
        return {
            "date": day,
            "complete": True,
            "temperature_min_c": night_min,
            "temperature_max_c": 12,
            "temperature_mean_c": 6,
            "night_min_c": night_min,
            "precipitation_mm": 0,
            "snowfall_cm": 0,
            "cloud_cover_mean_pct": 20,
            "cloud_cover_low_mean_pct": 5,
            "wind_speed_mean_kmh": 4,
            "wind_gust_max_kmh": 20,
            "solar_metric": {"variable": "sunshine_duration", "value": 100, "unit": "seconds"},
        }

    def make_long_range_hourly(self, days=36):
        start = datetime(2026, 9, 2)
        times = []
        for day in range(days):
            for hour in (0, 6, 12, 18):
                times.append((start + timedelta(days=day, hours=hour)).strftime("%Y-%m-%dT%H:%M"))
        hourly = {"time": times}
        for variable in pipeline.LONG_RANGE_VARIABLES:
            base_values = []
            for index in range(len(times)):
                day = index // 4
                if variable == "temperature_2m":
                    value = 12 - day * 0.1 + (index % 4) * 0.2
                elif variable == "precipitation":
                    value = 0.1 if day % 3 == 0 else 0
                elif variable == "snowfall":
                    value = 0.02 if day % 5 == 0 else 0
                else:
                    value = 20 + (day % 4)
                base_values.append(value)
            hourly[variable] = base_values
            for member in range(1, pipeline.LONG_RANGE_ENSEMBLE_MEMBERS):
                suffix = f"_member{member:02d}"
                hourly[f"{variable}{suffix}"] = [value + member * 0.01 for value in base_values]
        return hourly

    def make_ecmwf_ensemble_record(self, start_date=date(2026, 9, 23), days=15):
        start = datetime.combine(start_date, datetime.min.time())
        times = []
        for day in range(days):
            for hour in range(0, 24, 3):
                times.append((start + timedelta(days=day, hours=hour)).strftime("%Y-%m-%dT%H:%M"))
        hourly = {"time": times}
        for variable in pipeline.ENSEMBLE_VARIABLES:
            base_values = []
            for index, _ in enumerate(times):
                day = index // 8
                hour = (index % 8) * 3
                if variable == "temperature_2m":
                    value = 8 - day * 0.1 + hour * 0.02
                elif variable == "precipitation":
                    value = 0.2 if day % 4 == 0 else 0.0
                elif variable == "snowfall":
                    value = 0.05 if day % 5 == 0 else 0.0
                elif variable == "cloud_cover":
                    value = 30 + day % 3
                elif variable == "cloud_cover_low":
                    value = 10 + day % 2
                else:
                    value = 20 + day % 4
                base_values.append(value)
            hourly[variable] = base_values
            for member in range(1, 51):
                suffix = f"_member{member:02d}"
                hourly[f"{variable}{suffix}"] = [value + member * 0.01 for value in base_values]
        return {
            "status": "PASS",
            "response": {"timezone": "Asia/Shanghai", "utc_offset_seconds": 28800},
            "qa": {"final_status": "PASS"},
            "hourly": hourly,
        }

    def test_haversine_distance(self):
        self.assertEqual(pipeline.haversine_km(0, 0, 0, 0), 0)
        self.assertAlmostEqual(pipeline.haversine_km(48.69583, 86.78382, 48.75, 86.75), 6.514, places=2)

    def test_verified_filter_and_provisional_rejection(self):
        config = pipeline.load_config()
        active = pipeline.active_points(config)
        self.assertEqual(
            set(active),
            {
                "SQG_SHUANGQIAO",
                "BPG_CENTER",
                "LXL_HIGH_PASS",
                "JZG_TREESHENG",
                "JZG_NORILANG",
                "JZG_PRIMEVAL",
                "JZG_LONGHAI",
            },
        )
        # The shipped config carries no provisional point and no unverified
        # route slot, so the trust boundary is exercised on a synthetic copy.
        patched = copy.deepcopy(config)
        patched["points"]["JZG_TEST_PROVISIONAL"] = {
            "name": "九寨沟·候选高海拔点",
            "region": "jiuzhaigou",
            "subregion": "rize",
            "role": "route_high",
            "status": "PROVISIONAL",
            "latitude": 33.42,
            "longitude": 103.87,
        }
        patched["route_slots"]["LXL_TEST_ROUTE"] = {
            "name": "理小路高海拔延伸段",
            "status": "ROUTE_NOT_VERIFIED",
        }
        active = pipeline.active_points(patched)
        excluded = pipeline.excluded_points(patched)
        self.assertNotIn("JZG_TEST_PROVISIONAL", active)
        self.assertEqual(excluded["JZG_TEST_PROVISIONAL"]["status"], "PROVISIONAL")
        self.assertFalse(excluded["JZG_TEST_PROVISIONAL"]["usable_for_main_chain"])
        self.assertEqual(excluded["LXL_TEST_ROUTE"]["status"], "ROUTE_NOT_VERIFIED")
        self.assertFalse(excluded["LXL_TEST_ROUTE"]["usable_for_main_chain"])
        self.assertEqual(pipeline.excluded_points(config), {})

    def test_grid_cell_deduplication(self):
        record_a = {"response": {"grid_coordinate": {"latitude": 48.75, "longitude": 86.75}}}
        record_b = {"response": {"grid_coordinate": {"latitude": 48.7500001, "longitude": 86.75}}}
        self.assertEqual(pipeline.record_grid_cell_key(record_a), pipeline.record_grid_cell_key(record_b))

        samples = [
            {"status": "PASS", "grid_cell_key": "48.75,86.75", "daily": [self.make_day("2026-09-01", 4), self.make_day("2026-09-02", 6)]},
            {"status": "PASS", "grid_cell_key": "48.75,86.75", "daily": [self.make_day("2026-09-01", 4), self.make_day("2026-09-02", 6)]},
            {"status": "PASS", "grid_cell_key": "48.80,86.75", "daily": [self.make_day("2026-09-01", 6), self.make_day("2026-09-02", 6)]},
        ]
        summary = pipeline.spatial_region_summary("lixiaolu", samples)
        self.assertEqual(summary["unique_model_cells"], 2)
        self.assertEqual(summary["duplicate_requested_samples"], 1)
        self.assertEqual(summary["cold_pool_coverage"]["next_7d"]["cold_cells"], 1)
        self.assertEqual(summary["cold_pool_coverage"]["next_7d"]["total_cells"], 2)
        self.assertEqual(summary["cold_pool_coverage"]["next_7d"]["label"], "mixed")

    def test_jiuzhaigou_subregion_registry_keeps_provisional_points_out(self):
        config = pipeline.load_config()
        registry = pipeline.jiuzhaigou_subregion_registry(config)
        self.assertEqual(set(registry), {"shuzheng", "rize", "zezhawa"})
        self.assertEqual(registry["shuzheng"]["point_ids"], ["JZG_TREESHENG"])
        self.assertEqual(registry["rize"]["point_ids"], ["JZG_NORILANG", "JZG_PRIMEVAL"])
        self.assertEqual(registry["zezhawa"]["point_ids"], ["JZG_LONGHAI"])
        self.assertEqual(
            pipeline.jiuzhaigou_subregion_point_ids(config, "rize", verified_only=True),
            ["JZG_NORILANG", "JZG_PRIMEVAL"],
        )
        self.assertEqual(
            pipeline.jiuzhaigou_subregion_point_ids(config, "zezhawa", verified_only=True),
            ["JZG_LONGHAI"],
        )
        # A PROVISIONAL point listed in a subregion must stay out of the main
        # chain and out of the forward-history query set.
        patched = copy.deepcopy(config)
        patched["points"]["JZG_TEST_PROVISIONAL"] = {
            "name": "九寨沟·候选高海拔点",
            "region": "jiuzhaigou",
            "subregion": "rize",
            "role": "route_high",
            "status": "PROVISIONAL",
            "latitude": 33.42,
            "longitude": 103.87,
        }
        patched["jiuzhaigou_subregions"]["rize"]["point_ids"] = [
            "JZG_NORILANG",
            "JZG_PRIMEVAL",
            "JZG_TEST_PROVISIONAL",
        ]
        self.assertNotIn("JZG_TEST_PROVISIONAL", pipeline.active_points(patched))
        self.assertEqual(
            pipeline.jiuzhaigou_subregion_point_ids(patched, "rize", verified_only=True),
            ["JZG_NORILANG", "JZG_PRIMEVAL"],
        )
        self.assertNotIn("JZG_TEST_PROVISIONAL", pipeline.history_forward_point_ids(patched))

    def test_siguniang_subregion_registry_keeps_routes_out(self):
        config = pipeline.load_config()
        registry = pipeline.siguniang_subregion_registry(config)
        self.assertEqual(set(registry), {"shuangqiao", "bipenggou"})
        self.assertEqual(registry["shuangqiao"]["point_ids"], ["SQG_SHUANGQIAO"])
        self.assertEqual(registry["bipenggou"]["point_ids"], ["BPG_CENTER"])
        self.assertEqual(
            pipeline.siguniang_subregion_point_ids(config, "shuangqiao", verified_only=True),
            ["SQG_SHUANGQIAO"],
        )
        self.assertEqual(
            pipeline.siguniang_subregion_point_ids(config, "bipenggou", verified_only=True),
            ["BPG_CENTER"],
        )
        point_ids = pipeline.history_forward_point_ids(config)
        self.assertTrue({"SQG_SHUANGQIAO", "BPG_CENTER"}.issubset(point_ids))
        # The Lixiaolu high-pass route point is registered at region level only;
        # it must never leak into a Siguniang scenic-area subregion.
        subregion_points = [
            point_id
            for subregion_id in ("shuangqiao", "bipenggou")
            for point_id in registry[subregion_id]["point_ids"]
        ]
        self.assertEqual(sorted(subregion_points), ["BPG_CENTER", "SQG_SHUANGQIAO"])
        self.assertNotIn("LXL_HIGH_PASS", subregion_points)

    def test_siguniang_composite_uses_equal_subregion_weighting(self):
        definition = {
            "start_date": "2026-09-02",
            "end_date": "2026-09-09",
        }
        subregions = [
            {"status": "OK", "days_available": 8, "metrics": {"temperature_mean_c": 2}},
            {"status": "OK", "days_available": 8, "metrics": {"temperature_mean_c": 10}},
        ]
        composite = pipeline.equal_mean_subregion_window(subregions, definition, region_id="siguniang")
        self.assertEqual(composite["status"], "OK")
        self.assertEqual(composite["metrics"]["temperature_mean_c"], 6.0)
        self.assertIn("equal_mean_of_siguniang_subregions", composite["aggregation"])

    def test_jiuzhaigou_unique_grid_sampling_and_equal_grid_mean(self):
        config = pipeline.load_config()

        def record(point_id, grid, value):
            point = config["points"][point_id]
            daily = []
            for offset in range(8):
                day = self.make_day((date(2026, 9, 2) + timedelta(days=offset)).isoformat(), 2)
                day["temperature_mean_c"] = value
                day["temperature_min_c"] = value - 3
                day["temperature_max_c"] = value + 3
                day["night_min_c"] = value - 4
                daily.append(day)
            return {
                "point_id": point_id,
                "status": "PASS",
                "request": {"coordinate": {"latitude": point["latitude"], "longitude": point["longitude"]}},
                "response": {"grid_coordinate": grid, "returned_elevation": 1900, "timezone": "Asia/Shanghai"},
                "qa": {"final_status": "PASS", "grid_distance_km": 2, "grid_distance_limit_km": 14},
                "daily": daily,
            }

        records = {
            "JZG_TREESHENG": record("JZG_TREESHENG", {"latitude": 48.75, "longitude": 87.0}, 10),
            "JZG_NORILANG": record("JZG_NORILANG", {"latitude": 48.7500001, "longitude": 87.0}, 10),
            "JZG_LONGHAI": record("JZG_LONGHAI", {"latitude": 48.5, "longitude": 87.0}, 0),
        }
        sampling = pipeline.grid_sampling_summary(config, ["JZG_TREESHENG", "JZG_NORILANG", "JZG_LONGHAI"], records, minimum_verified_unique_grids=2)
        self.assertEqual(sampling["status"], "OK")
        self.assertEqual(sampling["unique_model_grids"], 2)
        self.assertEqual(sampling["point_to_grid"][0]["requested_coordinate"]["latitude"], 33.198939)
        self.assertEqual(sampling["unique_grid_mappings"][0]["point_ids"], ["JZG_TREESHENG", "JZG_NORILANG"])
        definition = pipeline.history_forward_windows_for_year(date(2026, 9, 2), 2026)["d0_7"]
        aggregate = pipeline.aggregate_grid_window(list(records.values()), definition)
        self.assertEqual(aggregate["status"], "OK")
        self.assertEqual(aggregate["metrics"]["temperature_mean_c"], 5.0)
        self.assertEqual(aggregate["metrics"]["night_min_mean_c"], 1.0)

    def test_jiuzhaigou_composite_preserves_equal_subregion_temperature_trend(self):
        definition = pipeline.history_forward_windows_for_year(date(2026, 9, 2), 2026)["d0_7"]
        items = [
            {
                "status": "OK",
                "expected_days": 8,
                "days_available": 8,
                "metrics": {
                    "temperature_mean_c": 6,
                    "temperature_trend": {
                        "first_3_days_mean_temperature_c": 8,
                        "last_3_days_mean_temperature_c": 4,
                        "last_3_minus_first_3_mean_temperature_c": -4,
                    },
                },
            },
            {
                "status": "OK",
                "expected_days": 8,
                "days_available": 8,
                "metrics": {
                    "temperature_mean_c": 8,
                    "temperature_trend": {
                        "first_3_days_mean_temperature_c": 7,
                        "last_3_days_mean_temperature_c": 8,
                        "last_3_minus_first_3_mean_temperature_c": 1,
                    },
                },
            },
            {
                "status": "OK",
                "expected_days": 8,
                "days_available": 8,
                "metrics": {
                    "temperature_mean_c": 10,
                    "temperature_trend": {
                        "first_3_days_mean_temperature_c": 9,
                        "last_3_days_mean_temperature_c": 12,
                        "last_3_minus_first_3_mean_temperature_c": 3,
                    },
                },
            },
        ]
        aggregate = pipeline.equal_mean_subregion_window(items, definition)
        self.assertEqual(aggregate["status"], "OK")
        self.assertEqual(aggregate["metrics"]["temperature_mean_c"], 8.0)
        self.assertEqual(aggregate["metrics"]["temperature_trend"]["first_3_days_mean_temperature_c"], 8.0)
        self.assertEqual(aggregate["metrics"]["temperature_trend"]["last_3_days_mean_temperature_c"], 8.0)
        self.assertEqual(aggregate["metrics"]["temperature_trend"]["last_3_minus_first_3_mean_temperature_c"], 0.0)
        self.assertEqual(aggregate["metrics"]["temperature_trend"]["direction"], "NEAR_FLAT")

    def test_lightweight_summary_is_schema_valid_and_contains_no_raw_series(self):
        config = pipeline.load_config()

        def fake_fetch(_client, **kwargs):
            point = kwargs["point"]
            start = date.fromisoformat(kwargs["params"]["start_date"])
            end = date.fromisoformat(kwargs["params"]["end_date"])
            daily = []
            cursor = start
            while cursor <= end:
                daily.append(self.make_day(cursor.isoformat(), 4))
                cursor += timedelta(days=1)
            return {
                "point_id": point["id"],
                "point": point,
                "status": "PASS",
                "source": "Open-Meteo",
                "endpoint": pipeline.OPEN_METEO_ENDPOINTS["history"],
                "model": "ECMWF IFS 9 km historical weather / analysis",
                "request": {"coordinate": {"latitude": point["latitude"], "longitude": point["longitude"]}},
                "response": {"grid_coordinate": {"latitude": 48.75, "longitude": 87.0}, "returned_elevation": 1900, "timezone": "Asia/Shanghai"},
                "qa": {"final_status": "PASS", "grid_distance_km": 2, "grid_distance_limit_km": pipeline.HISTORY_GRID_QA_LIMIT_KM},
                "solar_variable": "sunshine_duration",
                "daily": daily,
            }

        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(pipeline, "HISTORY_CACHE_DIR", Path(tmp)):
                with patch.object(pipeline, "fetch_point", side_effect=fake_fetch):
                    forward = pipeline.run_history_forward(
                        config,
                        object(),
                        "2026-09-02T00:00:00Z",
                        "2026-09-01",
                        date(2026, 9, 2),
                    )
        hres_points = {}
        for point_id in pipeline.history_forward_point_ids(config):
            point = config["points"][point_id]
            hres_points[point_id] = {
                "point_id": point_id,
                "status": "PASS",
                "request": {"coordinate": {"latitude": point["latitude"], "longitude": point["longitude"]}},
                "response": {"grid_coordinate": {"latitude": 48.75, "longitude": 87.0}, "returned_elevation": 1900, "timezone": "Asia/Shanghai"},
                "qa": {"final_status": "PASS", "grid_distance_km": 2, "grid_distance_limit_km": pipeline.HRES_GRID_QA_LIMIT_KM},
                "daily": [self.make_day((date(2026, 9, 2) + timedelta(days=offset)).isoformat(), 4) for offset in range(15)],
            }
        light = pipeline.build_phenology_weather_summary(
            config,
            "2026-09-02T00:00:00Z",
            "2026-09-01",
            date(2026, 9, 2),
            {"points": hres_points},
            forward,
        )
        with (ROOT / "schemas" / "phenology_weather_summary.schema.json").open(encoding="utf-8") as handle:
            schema = json.load(handle)
        self.assertEqual(list(Draft202012Validator(schema).iter_errors(light)), [])
        self.assertEqual(light["regions"]["jiuzhaigou"]["composite"]["years"]["2023"]["d0_7"]["start_date"], "2023-09-02")
        self.assertEqual(light["regions"]["jiuzhaigou"]["composite"]["years"]["2023"]["d0_7"]["end_date"], "2023-09-09")
        self.assertEqual(light["regions"]["jiuzhaigou"]["composite"]["years"]["2026"]["d0_7"]["status"], "OK")
        self.assertEqual(light["regions"]["jiuzhaigou"]["composite"]["years"]["2026"]["d8_15"]["status"], "PARTIAL")
        self.assertIsNotNone(light["regions"]["jiuzhaigou"]["composite"]["years"]["2026"]["d8_15"]["temperature_mean_c"])
        self.assertEqual(set(light["regions"]["siguniang"]["subregions"]), {"shuangqiao", "bipenggou"})
        self.assertEqual(
            light["regions"]["siguniang"]["composite"]["years"]["2023"]["d0_7"]["start_date"],
            "2023-09-02",
        )
        self.assertEqual(
            light["regions"]["siguniang"]["composite"]["years"]["2026"]["d0_7"]["status"],
            "OK",
        )
        for region_id in ("jiuzhaigou", "siguniang"):
            region = light["regions"][region_id]
            self.assertTrue(region["usable_for_main_chain"])
            self.assertTrue(region["composite"]["usable_for_main_chain"])
            d0_7 = region["composite"]["years"]["2026"]["d0_7"]
            d8_15 = region["composite"]["years"]["2026"]["d8_15"]
            d16 = region["composite"]["years"]["2026"]["d16_to_11_01"]
            self.assertTrue(d0_7["usable_for_main_chain"])
            self.assertFalse(d8_15["usable_for_main_chain"])
            self.assertTrue(d8_15["usable_for_trend_reference"])
            self.assertEqual(d8_15["availability_note"], "9/10–16预测，9/17待补")
            self.assertEqual(d16["status"], "INVALID")
            self.assertFalse(d16["usable_for_main_chain"])
            self.assertFalse(d16["usable_for_trend_reference"])
        serialized = json.dumps(light, ensure_ascii=False).lower()
        self.assertNotIn('"hourly"', serialized)
        self.assertNotIn('"daily"', serialized)
        for forbidden in ("actual_phenology_lead_days", "yellow_leaf_percentage", "旅游建议"):
            self.assertNotIn(forbidden, serialized)

    def test_edge_missing_rows_are_trimmed_and_audited(self):
        payload = self.make_payload()
        payload["hourly"]["time"] = ["a", "b", "c"]
        payload["hourly"]["temperature_2m"] = [None, 1, None]
        payload["hourly"]["precipitation"] = [None, 0, None]
        trimmed, audit = pipeline.trim_incomplete_edge_rows(payload, ["temperature_2m", "precipitation"])
        self.assertEqual(trimmed["hourly"]["time"], ["b"])
        self.assertEqual(audit["leading_missing_rows"], 1)
        self.assertEqual(audit["trailing_missing_rows"], 1)
        self.assertEqual(audit["horizon_status"], "TRUNCATED_EDGE_MISSING")

    def test_model_timezone_and_grid_qa_failures(self):
        point = {"latitude": 48.75, "longitude": 86.75}
        bad_model = pipeline.validate_payload(
            self.make_payload(model="wrong_model"), point, "ecmwf_ifs", ["temperature_2m", "precipitation"], 1
        )
        self.assertFalse(bad_model["valid"])
        self.assertIn("MODEL_MISMATCH", bad_model["reason"])

        bad_timezone = pipeline.validate_payload(
            self.make_payload(timezone="UTC", offset=0), point, "ecmwf_ifs", ["temperature_2m", "precipitation"], 1
        )
        self.assertFalse(bad_timezone["valid"])
        self.assertIn("TIMEZONE_MISMATCH", bad_timezone["reason"])

        far_grid = self.make_payload()
        far_grid["latitude"] = 0
        far_grid["longitude"] = 0
        far_grid_result = pipeline.validate_payload(
            far_grid, point, "ecmwf_ifs", ["temperature_2m", "precipitation"], 1
        )
        self.assertFalse(far_grid_result["valid"])
        self.assertIn("GRID_REPRESENTATIVENESS_FAIL", far_grid_result["reason"])

    def test_site_hres_grid_limit_falls_back_to_the_module_constant(self):
        self.assertEqual(pipeline.site_hres_grid_qa_limit_km({}), pipeline.HRES_GRID_QA_LIMIT_KM)
        self.assertEqual(
            pipeline.site_hres_grid_qa_limit_km({"grid_qa": {}}), pipeline.HRES_GRID_QA_LIMIT_KM
        )
        self.assertEqual(
            pipeline.site_hres_grid_qa_limit_km({"grid_qa": {"hres_limit_km": None}}),
            pipeline.HRES_GRID_QA_LIMIT_KM,
        )

    def test_site_hres_grid_limit_accepts_an_explicit_site_value(self):
        self.assertEqual(
            pipeline.site_hres_grid_qa_limit_km({"grid_qa": {"hres_limit_km": 16.5}}), 16.5
        )
        self.assertEqual(
            pipeline.site_hres_grid_qa_limit_km({"grid_qa": {"hres_limit_km": "17"}}), 17.0
        )

    def test_site_hres_grid_limit_rejects_malformed_values(self):
        with self.assertRaises(ValueError):
            pipeline.site_hres_grid_qa_limit_km({"grid_qa": {"hres_limit_km": 0}})
        with self.assertRaises(ValueError):
            pipeline.site_hres_grid_qa_limit_km({"grid_qa": {"hres_limit_km": -1}})
        with self.assertRaises(ValueError):
            pipeline.site_hres_grid_qa_limit_km({"grid_qa": {"hres_limit_km": "wide"}})
        with self.assertRaises(ValueError):
            pipeline.site_hres_grid_qa_limit_km({"grid_qa": "wide"})

    def test_hres_uses_the_site_grid_limit_and_the_missing_point_still_fails(self):
        config = {
            "namespace": "siguniang_jiuzhaigou",
            "grid_qa": {"hres_limit_km": 16.5},
            "route_slots": {},
            "points": {
                "JZG_PRIMEVAL": {
                    "name": "九寨沟·原始森林",
                    "region": "jiuzhaigou",
                    "status": "VERIFIED",
                    "latitude": 33.347,
                    "longitude": 103.865,
                }
            },
        }
        seen = {}

        def stub_fetch(client, **kwargs):
            seen["grid_limit_km"] = kwargs["grid_limit_km"]
            return {
                "point_id": kwargs["point"]["id"],
                "status": "PASS",
                "variable_status": {},
                "qa": {"final_status": "PASS"},
            }

        with patch.object(pipeline, "fetch_point", side_effect=stub_fetch):
            record = pipeline.run_hres(config, client=None, generated_at="2026-09-24T00:00:00Z", data_date="2026-09-23")

        self.assertEqual(seen["grid_limit_km"], 16.5)
        self.assertEqual(record["status"], "OK")
        self.assertEqual(record["successful_points"], 1)
        self.assertEqual(record["failed_points"], 0)

    def test_hres_keeps_the_default_limit_when_the_site_declares_none(self):
        config = {
            "namespace": "siguniang_jiuzhaigou",
            "route_slots": {},
            "points": {
                "JZG_PRIMEVAL": {
                    "name": "九寨沟·原始森林",
                    "region": "jiuzhaigou",
                    "status": "VERIFIED",
                    "latitude": 33.347,
                    "longitude": 103.865,
                }
            },
        }
        seen = {}

        def stub_fetch(client, **kwargs):
            seen["grid_limit_km"] = kwargs["grid_limit_km"]
            return {"point_id": "JZG_PRIMEVAL", "status": "PASS", "variable_status": {}, "qa": {}}

        with patch.object(pipeline, "fetch_point", side_effect=stub_fetch):
            pipeline.run_hres(config, client=None, generated_at="2026-09-24T00:00:00Z", data_date="2026-09-23")

        self.assertEqual(seen["grid_limit_km"], pipeline.HRES_GRID_QA_LIMIT_KM)

    def test_checked_in_config_declares_a_site_grid_limit_above_the_default(self):
        config = json.loads((ROOT / "config" / "points.json").read_text(encoding="utf-8"))
        self.assertIn("grid_qa", config)
        limit = pipeline.site_hres_grid_qa_limit_km(config)
        sampling = config["sampling"]
        sampling_basis = float(sampling["radius_km"]) + float(sampling["grid_qa_extra_allowance_km"])
        # Wider than the high-latitude default, which rejects the two low-latitude
        # points at 15.19 km and 14.02 km ...
        self.assertGreater(limit, pipeline.HRES_GRID_QA_LIMIT_KM)
        # ... but no looser than the tolerance the pipeline already grants its own
        # 12 km spatial samples, so the site cannot widen HRES without bound.
        self.assertLessEqual(limit, sampling_basis)

    def test_missing_api_data_is_invalid(self):
        payload = self.make_payload()
        del payload["hourly"]["precipitation"]
        result = pipeline.validate_payload(
            payload, {"latitude": 48.75, "longitude": 86.75}, "ecmwf_ifs", ["temperature_2m", "precipitation"], 1
        )
        self.assertFalse(result["valid"])
        self.assertIn("MISSING_DATA:precipitation", result["reason"])

    def test_threshold_and_consecutive_cold_night_metrics(self):
        days = [
            self.make_day("2026-08-25", 4),
            self.make_day("2026-08-26", 3),
            self.make_day("2026-08-27", 6),
            self.make_day("2026-08-29", 1),
        ]
        metrics = pipeline.period_metrics(days)
        self.assertEqual(metrics["threshold_nights"]["below_5_c"], 3)
        below_5 = metrics["consecutive_cold_nights"]["below_5_c"]
        self.assertEqual(below_5["max_consecutive"], 2)
        self.assertEqual(below_5["sequences"], [
            {"start_date": "2026-08-25", "end_date": "2026-08-26", "nights": 2},
            {"start_date": "2026-08-29", "end_date": "2026-08-29", "nights": 1},
        ])

    def test_history_year_matching_and_weather_only_boundary(self):
        config = pipeline.load_config()
        requested_coordinate = {"latitude": 48.69583, "longitude": 86.78382}
        returned_grid = {"latitude": 48.75, "longitude": 86.75}
        point_results = {
            "SQG_SHUANGQIAO": {
                "point": {"region": "siguniang"},
                "years": {
                    str(year): {
                        "status": "PASS",
                        "daily": [self.make_day(f"{year}-08-25", 8 if year < 2026 else 3)] * 3,
                        "request": {"coordinate": requested_coordinate},
                        "response": {"grid_coordinate": returned_grid},
                    }
                    for year in (2023, 2024, 2025, 2026)
                },
            }
        }
        comparison = pipeline.build_history_comparison(config, point_results, "2026-08-27")
        self.assertEqual(set(comparison["points"]["SQG_SHUANGQIAO"]["metrics"]), {"2023", "2024", "2025", "2026"})
        self.assertEqual(comparison["points"]["SQG_SHUANGQIAO"]["metrics"]["2025"]["period_start"], "2025-08-25")
        self.assertEqual(comparison["points"]["SQG_SHUANGQIAO"]["metrics"]["2026"]["period_start"], "2026-08-25")
        self.assertEqual(comparison["points"]["SQG_SHUANGQIAO"]["same_grid_qa"]["checked_years"], ["2023", "2024", "2025", "2026"])
        self.assertEqual(comparison["points"]["SQG_SHUANGQIAO"]["same_grid_qa"]["final_status"], "PASS")
        self.assertIn("delta_2026_minus_2023", comparison["points"]["SQG_SHUANGQIAO"])
        self.assertIn("delta_2026_minus_2024", comparison["points"]["SQG_SHUANGQIAO"])
        self.assertIn("weather_driver_vs_2023", comparison["points"]["SQG_SHUANGQIAO"])
        self.assertIn("weather_driver_vs_2024", comparison["points"]["SQG_SHUANGQIAO"])
        self.assertEqual(comparison["regions"]["siguniang"]["status"], "OK")
        self.assertNotIn("actual_phenology_lead_days", comparison["points"]["SQG_SHUANGQIAO"])

    def test_history_grid_mismatch_fails_all_year_comparison(self):
        config = pipeline.load_config()
        point_results = {
            "SQG_SHUANGQIAO": {
                "point": {"region": "siguniang"},
                "years": {
                    str(year): {
                        "status": "PASS",
                        "daily": [self.make_day(f"{year}-08-25", 4)] * 3,
                        "request": {"coordinate": {"latitude": 48.69583, "longitude": 86.78382}},
                        "response": {
                            "grid_coordinate": {"latitude": 48.75, "longitude": 86.75}
                            if year != 2024
                            else {"latitude": 48.80, "longitude": 86.75}
                        },
                    }
                    for year in (2023, 2024, 2025, 2026)
                },
            }
        }
        comparison = pipeline.build_history_comparison(config, point_results, "2026-08-27")
        point = comparison["points"]["SQG_SHUANGQIAO"]
        self.assertEqual(point["status"], "FAILED")
        self.assertEqual(point["same_grid_qa"]["status"], "FAIL")
        self.assertEqual(point["same_grid_qa"]["final_status"], "FAILED")
        self.assertEqual(point["same_grid_qa"]["reason"], "HISTORICAL_GRID_MISMATCH")
        self.assertEqual(point["delta_2026_minus_2023"], {})

    def test_history_year_configuration_matches_the_config(self):
        self.assertEqual(pipeline.history_years_for_config(pipeline.load_config()), (2023, 2024, 2025, 2026))

    def test_run_history_requests_all_configured_years(self):
        config = pipeline.load_config()
        requests = []

        def fake_fetch(_client, **kwargs):
            requests.append(kwargs["params"])
            point = kwargs["point"]
            day = kwargs["params"]["start_date"]
            hourly = {
                "time": [f"{day}T{hour:02d}:00" for hour in range(24)],
                "temperature_2m": [5] * 24,
                "precipitation": [0] * 24,
                "snowfall": [0] * 24,
                "cloud_cover": [20] * 24,
                "cloud_cover_low": [5] * 24,
                "wind_speed_10m": [4] * 24,
                "wind_gusts_10m": [20] * 24,
            }
            return {
                "point_id": point["id"],
                "point": point,
                "status": "PASS",
                "hourly": hourly,
                "request": {
                    "coordinate": {"latitude": point["latitude"], "longitude": point["longitude"]},
                    "parameters": kwargs["params"],
                },
                "response": {
                    "grid_coordinate": {"latitude": 48.75, "longitude": 86.75},
                    "returned_elevation": 1000,
                    "timezone": "Asia/Shanghai",
                    "utc_offset_seconds": 28800,
                },
                "qa": {
                    "final_status": "PASS",
                    "grid_distance_km": 0,
                    "grid_distance_limit_km": pipeline.HISTORY_GRID_QA_LIMIT_KM,
                },
                "solar_variable": None,
            }

        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(pipeline, "HISTORY_CACHE_DIR", Path(tmp)):
                with patch.object(pipeline, "fetch_point", side_effect=fake_fetch):
                    history = pipeline.run_history(
                        config,
                        object(),
                        "2026-08-28T00:00:00Z",
                        "2026-08-27",
                        date(2026, 8, 27),
                    )

        self.assertEqual(history["history_years"], [2023, 2024, 2025, 2026])
        self.assertEqual(len(requests), len(pipeline.active_points(config)) * 4)
        self.assertEqual(sorted({item["start_date"][:4] for item in requests}), ["2023", "2024", "2025", "2026"])
        self.assertTrue(all(item["models"] == "ecmwf_ifs" for item in requests))
        self.assertTrue(all(item["cell_selection"] == "nearest" for item in requests))
        self.assertTrue(all(item["elevation"] == "nan" for item in requests))
        self.assertTrue(all(item["timezone"] == "Asia/Shanghai" for item in requests))

    def test_history_cache_hit_does_not_repeat_api_request(self):
        config = pipeline.load_config()
        point = pipeline.active_points(config)["SQG_SHUANGQIAO"]
        requests = []

        def fake_fetch(_client, **kwargs):
            requests.append(kwargs["params"].copy())
            return self.make_history_forward_record(kwargs["point"], kwargs["params"])

        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            with patch.object(pipeline, "fetch_point", side_effect=fake_fetch):
                first = pipeline.history_cache_record_or_fetch(
                    config,
                    object(),
                    point,
                    2025,
                    "2025-09-01",
                    "2025-09-03",
                    cache_dir=cache_dir,
                    log_label="SQG_SHUANGQIAO:HISTORY CACHE TEST",
                )
                second = pipeline.history_cache_record_or_fetch(
                    config,
                    object(),
                    point,
                    2025,
                    "2025-09-01",
                    "2025-09-03",
                    cache_dir=cache_dir,
                    log_label="SQG_SHUANGQIAO:HISTORY CACHE TEST",
                )
            self.assertEqual(first["status"], "PASS")
            self.assertEqual(second["status"], "PASS")
            self.assertEqual(second["history_cache"]["status"], "HIT")
            self.assertEqual(len(requests), 1)
            self.assertEqual(
                pipeline.history_cache_path(config, 2025, "SQG_SHUANGQIAO", cache_dir),
                cache_dir / "siguniang_jiuzhaigou" / "2025" / "SQG_SHUANGQIAO.json",
            )
            with pipeline.history_cache_path(config, 2025, "SQG_SHUANGQIAO", cache_dir).open(encoding="utf-8") as handle:
                cache = json.load(handle)
            with (ROOT / "schemas" / "history_cache.schema.json").open(encoding="utf-8") as handle:
                schema = json.load(handle)
            self.assertEqual(list(Draft202012Validator(schema).iter_errors(cache)), [])

    def test_history_cache_only_fills_missing_dates(self):
        config = pipeline.load_config()
        point = pipeline.active_points(config)["SQG_SHUANGQIAO"]
        requests = []

        def fake_fetch(_client, **kwargs):
            requests.append(kwargs["params"].copy())
            return self.make_history_forward_record(kwargs["point"], kwargs["params"])

        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(pipeline, "fetch_point", side_effect=fake_fetch):
                pipeline.history_cache_record_or_fetch(
                    config,
                    object(),
                    point,
                    2025,
                    "2025-09-01",
                    "2025-09-03",
                    cache_dir=Path(tmp),
                    log_label="SQG_SHUANGQIAO:HISTORY CACHE TEST",
                )
                filled = pipeline.history_cache_record_or_fetch(
                    config,
                    object(),
                    point,
                    2025,
                    "2025-09-01",
                    "2025-09-05",
                    cache_dir=Path(tmp),
                    log_label="SQG_SHUANGQIAO:HISTORY CACHE TEST",
                )
            self.assertEqual(len(requests), 2)
            self.assertEqual(requests[1]["start_date"], "2025-09-04")
            self.assertEqual(requests[1]["end_date"], "2025-09-05")
            self.assertEqual(filled["status"], "PASS")
            self.assertEqual(filled["history_cache"]["status"], "FILLED")
            self.assertEqual(filled["history_cache"]["missing_date_count_before_fetch"], 2)

    def test_history_cache_identity_mismatch_is_invalid_without_refetch(self):
        config = pipeline.load_config()
        point = pipeline.active_points(config)["SQG_SHUANGQIAO"]
        requests = []

        def fake_fetch(_client, **kwargs):
            requests.append(kwargs["params"].copy())
            return self.make_history_forward_record(kwargs["point"], kwargs["params"])

        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            with patch.object(pipeline, "fetch_point", side_effect=fake_fetch):
                pipeline.history_cache_record_or_fetch(
                    config,
                    object(),
                    point,
                    2025,
                    "2025-09-01",
                    "2025-09-03",
                    cache_dir=cache_dir,
                    log_label="SQG_SHUANGQIAO:HISTORY CACHE TEST",
                )
            cache_path = pipeline.history_cache_path(config, 2025, "SQG_SHUANGQIAO", cache_dir)
            with cache_path.open(encoding="utf-8") as handle:
                cache = json.load(handle)
            cache["identity"]["timezone"] = "UTC"
            with cache_path.open("w", encoding="utf-8") as handle:
                json.dump(cache, handle)
            with patch.object(pipeline, "fetch_point", side_effect=fake_fetch):
                invalid = pipeline.history_cache_record_or_fetch(
                    config,
                    object(),
                    point,
                    2025,
                    "2025-09-01",
                    "2025-09-03",
                    cache_dir=cache_dir,
                    log_label="SQG_SHUANGQIAO:HISTORY CACHE TEST",
                )
            self.assertEqual(len(requests), 1)
            self.assertEqual(invalid["status"], "INVALID")
            self.assertEqual(invalid["history_cache"]["status"], "INVALID")
            self.assertEqual(invalid["qa"]["reason"], "HISTORY_CACHE_IDENTITY_MISMATCH")
            self.assertIn("timezone", invalid["history_cache"]["identity_mismatches"])

    def test_refresh_history_forces_full_cache_revalidation(self):
        config = pipeline.load_config()
        point = pipeline.active_points(config)["SQG_SHUANGQIAO"]
        requests = []

        def fake_fetch(_client, **kwargs):
            requests.append(kwargs["params"].copy())
            return self.make_history_forward_record(kwargs["point"], kwargs["params"])

        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(pipeline, "fetch_point", side_effect=fake_fetch):
                pipeline.history_cache_record_or_fetch(
                    config,
                    object(),
                    point,
                    2025,
                    "2025-09-01",
                    "2025-09-03",
                    cache_dir=Path(tmp),
                    log_label="SQG_SHUANGQIAO:HISTORY CACHE TEST",
                )
                refreshed = pipeline.history_cache_record_or_fetch(
                    config,
                    object(),
                    point,
                    2025,
                    "2025-09-01",
                    "2025-09-03",
                    refresh_history=True,
                    cache_dir=Path(tmp),
                    log_label="SQG_SHUANGQIAO:HISTORY CACHE TEST",
                )
            self.assertEqual(len(requests), 2)
            self.assertEqual(requests[1]["start_date"], "2025-09-01")
            self.assertEqual(requests[1]["end_date"], "2025-09-03")
            self.assertEqual(refreshed["status"], "PASS")
            self.assertEqual(refreshed["history_cache"]["status"], "REFRESHED")

    def test_history_forward_reuses_cache_warmed_by_history_comparison(self):
        config = pipeline.load_config()
        requests = []

        def fake_fetch(_client, **kwargs):
            requests.append(kwargs["params"].copy())
            return self.make_history_forward_record(kwargs["point"], kwargs["params"])

        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(pipeline, "HISTORY_CACHE_DIR", Path(tmp)), patch.object(pipeline, "log"):
                with patch.object(pipeline, "fetch_point", side_effect=fake_fetch):
                    history = pipeline.run_history(
                        config,
                        object(),
                        "2026-09-02T00:00:00Z",
                        "2026-09-01",
                        date(2026, 9, 1),
                        forward_anchor_date=date(2026, 9, 2),
                    )
                calls_after_history = len(requests)
                with patch.object(pipeline, "fetch_point", side_effect=fake_fetch):
                    forward = pipeline.run_history_forward(
                        config,
                        object(),
                        "2026-09-02T00:00:00Z",
                        "2026-09-01",
                        date(2026, 9, 2),
                    )
            self.assertEqual(calls_after_history, len(pipeline.active_points(config)) * 4)
            self.assertEqual(len(requests), calls_after_history)
            self.assertEqual(history["history_cache"]["api_requests"], calls_after_history)
            self.assertEqual(forward["history_cache"]["api_requests"], 0)
            self.assertEqual(forward["history_cache"]["cache_hits"], len(pipeline.history_forward_point_ids(config)) * 3)

    def test_long_range_reference_reuses_historical_cache(self):
        config = pipeline.load_config()
        point = pipeline.active_points(config)["SQG_SHUANGQIAO"]
        requests = []

        def fake_fetch(_client, **kwargs):
            requests.append(kwargs["params"].copy())
            return self.make_history_forward_record(kwargs["point"], kwargs["params"])

        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            with patch.object(pipeline, "fetch_point", side_effect=fake_fetch):
                pipeline.history_cache_record_or_fetch(
                    config,
                    object(),
                    point,
                    2025,
                    "2025-09-19",
                    "2025-09-21",
                    cache_dir=cache_dir,
                    log_label="SQG_SHUANGQIAO:HISTORY CACHE TEST",
                )
                reference = pipeline.run_long_range_reference(
                    config,
                    point,
                    object(),
                    date(2026, 9, 3),
                    "2026-09-03T00:00:00Z",
                    "2026-09-02",
                    cache_dir=cache_dir,
                )
            self.assertEqual(reference["status"], "PASS")
            self.assertEqual(reference["record"]["history_cache"]["status"], "FILLED")
            self.assertEqual(len(requests), 2)
            self.assertEqual(requests[1]["start_date"], "2025-09-22")
            self.assertEqual(requests[1]["end_date"], "2025-10-08")

    def make_history_forward_record(self, point, params, *, grid=None):
        start = date.fromisoformat(params["start_date"])
        end = date.fromisoformat(params["end_date"])
        daily = []
        cursor = start
        while cursor <= end:
            daily.append(self.make_day(cursor.isoformat(), 4))
            cursor += timedelta(days=1)
        returned_grid = grid or {
            "latitude": round(point["latitude"], 6),
            "longitude": round(point["longitude"], 6),
        }
        return {
            "point_id": point["id"],
            "point": point,
            "status": "PASS",
            "source": "Open-Meteo",
            "endpoint": pipeline.OPEN_METEO_ENDPOINTS["history"],
            "model": "ECMWF IFS 9 km historical weather / analysis",
            "request": {
                "coordinate": {"latitude": point["latitude"], "longitude": point["longitude"]},
                "parameters": params,
            },
            "response": {
                "grid_coordinate": returned_grid,
                "returned_elevation": 1000,
                "timezone": "Asia/Shanghai",
                "utc_offset_seconds": 28800,
            },
            "qa": {
                "final_status": "PASS",
                "grid_distance_km": 0,
                "grid_distance_limit_km": pipeline.HISTORY_GRID_QA_LIMIT_KM,
            },
            "solar_variable": "sunshine_duration",
            "daily": daily,
        }

    def test_history_forward_window_boundaries_roll_and_cutoff(self):
        definitions = pipeline.history_forward_window_definitions(date(2026, 9, 2))
        self.assertEqual(
            [(item["window"], item["start_date"], item["end_date"]) for item in definitions],
            [
                ("d0_7", "2026-09-02", "2026-09-09"),
                ("d8_15", "2026-09-10", "2026-09-17"),
                ("d16_to_11_01", "2026-09-18", "2026-11-01"),
            ],
        )
        translated = pipeline.history_forward_windows_for_year(date(2026, 9, 2), 2023)
        self.assertEqual(translated["d0_7"]["start_date"], "2023-09-02")
        self.assertEqual(translated["d16_to_11_01"]["end_date"], "2023-11-01")
        late = pipeline.history_forward_window_definitions(date(2026, 10, 25))
        self.assertEqual(late[0]["end_date"], "2026-11-01")
        self.assertEqual(late[0]["status"], "OK")
        self.assertEqual(late[1]["status"], pipeline.HISTORY_FORWARD_WINDOW_NOT_APPLICABLE)
        self.assertEqual(late[2]["status"], pipeline.HISTORY_FORWARD_WINDOW_NOT_APPLICABLE)
        self.assertEqual(late[1]["reason"], "WINDOW_AFTER_CUTOFF")
        for item in late:
            for field in ("requested_start_date", "requested_end_date", "start_date", "end_date"):
                if item.get(field):
                    self.assertLessEqual(item[field], "2026-11-01")

    def test_history_forward_fetches_three_years_jiuzhaigou_subregion_points_and_clips_cutoff(self):
        config = pipeline.load_config()
        requests = []

        def fake_fetch(_client, **kwargs):
            requests.append(kwargs["params"])
            return self.make_history_forward_record(kwargs["point"], kwargs["params"])

        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(pipeline, "HISTORY_CACHE_DIR", Path(tmp)):
                with patch.object(pipeline, "fetch_point", side_effect=fake_fetch):
                    result = pipeline.run_history_forward(
                        config,
                        object(),
                        "2026-09-02T00:00:00Z",
                        "2026-09-01",
                        date(2026, 9, 2),
                    )
        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["history_years"], [2023, 2024, 2025])
        self.assertEqual(result["forecast_date"], "2026-09-02")
        expected_points = pipeline.history_forward_point_ids(config)
        self.assertEqual(result["expected_fetches"], len(expected_points) * 3)
        self.assertEqual(result["successful_fetches"], len(expected_points) * 3)
        self.assertEqual(len(requests), len(expected_points) * 3)
        self.assertEqual(set(result["points"]), set(expected_points))
        for params in requests:
            self.assertEqual(params["models"], "ecmwf_ifs")
            self.assertEqual(params["cell_selection"], "nearest")
            self.assertEqual(params["elevation"], "nan")
            self.assertEqual(params["timezone"], "Asia/Shanghai")
            self.assertEqual(params["end_date"][5:], "11-01")
        for region_id in ("lixiaolu", "jiuzhaigou", "siguniang"):
            region = result["regions"][region_id]
            self.assertEqual(region["status"], "OK")
            self.assertTrue(region["cross_year_comparison_usable"])
            for year in ("2023", "2024", "2025"):
                self.assertEqual(region["years"][year]["d0_7"]["start_date"], f"{year}-09-02")
                self.assertEqual(region["years"][year]["d0_7"]["end_date"], f"{year}-09-09")
                self.assertEqual(region["years"][year]["d8_15"]["start_date"], f"{year}-09-10")
                self.assertEqual(region["years"][year]["d8_15"]["end_date"], f"{year}-09-17")
                self.assertEqual(region["years"][year]["d16_to_11_01"]["end_date"], f"{year}-11-01")
                self.assertTrue(all(day["date"] <= f"{year}-11-01" for day in region["years"][year]["d16_to_11_01"]["daily"]))
            self.assertEqual(region["same_grid_qa"]["checked_years"], ["2023", "2024", "2025"])
            self.assertEqual(region["same_grid_qa"]["final_status"], "PASS")
        self.assertEqual(result["regions"]["jiuzhaigou"]["subregions"]["shuzheng"]["status"], "OK")
        self.assertEqual(result["regions"]["jiuzhaigou"]["subregions"]["rize"]["status"], "OK")
        self.assertEqual(result["regions"]["jiuzhaigou"]["subregions"]["zezhawa"]["status"], "OK")
        self.assertEqual(result["regions"]["jiuzhaigou"]["composite"]["status"], "OK")
        for subregion_id in ("shuzheng", "rize", "zezhawa"):
            sampling = result["regions"]["jiuzhaigou"]["subregions"][subregion_id]["sampling"]
            self.assertTrue(sampling["same_unique_grid_set_across_years"])
            self.assertEqual(
                set(sampling["by_year"]),
                {"2023", "2024", "2025"},
            )
        siguniang = result["regions"]["siguniang"]
        self.assertEqual(siguniang["status"], "OK")
        self.assertTrue(siguniang["usable_for_main_chain"])
        self.assertEqual(set(siguniang["subregions"]), {"shuangqiao", "bipenggou"})
        self.assertEqual(siguniang["composite"]["status"], "OK")
        self.assertEqual(set(siguniang["subregions"]["shuangqiao"]["point_ids"]), {"SQG_SHUANGQIAO"})
        self.assertEqual(set(siguniang["subregions"]["bipenggou"]["point_ids"]), {"BPG_CENTER"})
        self.assertIn("SQG_SHUANGQIAO", result["points"])

    def test_history_forward_same_grid_failure_blocks_cross_year_comparison(self):
        config = pipeline.load_config()
        def fake_fetch(_client, **kwargs):
            year = int(kwargs["params"]["start_date"][:4])
            grid = {"latitude": 48.75, "longitude": 86.75} if year != 2024 else {"latitude": 48.80, "longitude": 86.75}
            return self.make_history_forward_record(kwargs["point"], kwargs["params"], grid=grid)

        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(pipeline, "HISTORY_CACHE_DIR", Path(tmp)):
                with patch.object(pipeline, "fetch_point", side_effect=fake_fetch):
                    result = pipeline.run_history_forward(
                        config,
                        object(),
                        "2026-09-02T00:00:00Z",
                        "2026-09-01",
                        date(2026, 9, 2),
                    )
        self.assertEqual(result["status"], "FAILED")
        point = result["points"]["SQG_SHUANGQIAO"]
        self.assertEqual(point["same_grid_qa"]["final_status"], "FAILED")
        self.assertFalse(point["cross_year_comparison_usable"])
        self.assertEqual(point["same_grid_qa"]["pairwise"]["2023_vs_2024"], "FAIL")
        self.assertEqual(result["regions"]["lixiaolu"]["status"], "FAILED")
        self.assertEqual(result["regions"]["siguniang"]["status"], "FAILED")
        self.assertEqual(result["regions"]["siguniang"]["composite"]["status"], "INVALID")
        self.assertFalse(result["regions"]["siguniang"]["composite"]["cross_year_comparison_usable"])

    def test_history_forward_status_failure_does_not_hide_short_chain(self):
        config = pipeline.load_config()
        modules = {name: {"status": "OK"} for name in ("hres", "history", "ensemble", "gfs", "single_runs", "spatial_sampling")}
        modules["history_forward"] = {"status": "FAILED", "error": "TEST"}
        modules["long_range"] = {"status": "OK"}
        status = pipeline.build_status(config, "2026-09-02T00:00:00Z", "2026-09-01", modules)
        self.assertEqual(status["pipeline_status"], "PARTIAL")
        self.assertEqual(status["modules"]["history_forward"], "FAILED")
        self.assertEqual(status["modules"]["hres"], "OK")

    def test_history_forward_schema_and_weather_only_boundary(self):
        config = pipeline.load_config()
        record = self.make_history_forward_record(pipeline.active_points(config)["SQG_SHUANGQIAO"], {
            "models": "ecmwf_ifs",
            "cell_selection": "nearest",
            "elevation": "nan",
            "timezone": "Asia/Shanghai",
            "start_date": "2023-09-02",
            "end_date": "2023-11-01",
        })
        windows = pipeline.history_forward_windows_for_year(date(2026, 9, 2), 2023)
        for key, definition in windows.items():
            record[key] = pipeline.history_forward_window_summary(record["daily"], definition)
        qa = pipeline.history_forward_same_grid_qa({str(year): record for year in (2023, 2024, 2025)})
        result = pipeline.module_header(
            "history_forward",
            "2026-09-02T00:00:00Z",
            "2026-09-01",
            "OK",
            forecast_date="2026-09-02",
            anchor_date="2026-09-02",
            cutoff_date="2026-11-01",
            history_years=[2023, 2024, 2025],
            window_definitions=pipeline.history_forward_window_definitions(date(2026, 9, 2)),
            points={"SQG_SHUANGQIAO": {"point_id": "SQG_SHUANGQIAO", "status": "OK", "same_grid_qa": qa, "years": {str(year): record for year in (2023, 2024, 2025)}}},
            regions={"siguniang": {"region": "siguniang", "status": "UNAVAILABLE", "usable_for_main_chain": False, "cross_year_comparison_usable": False, "same_grid_qa": None, "years": {}}},
            excluded_points={},
        )
        with (ROOT / "schemas" / "history_forward.schema.json").open(encoding="utf-8") as handle:
            schema = json.load(handle)
        errors = list(Draft202012Validator(schema).iter_errors(result))
        self.assertEqual(errors, [])
        text = json.dumps(result, ensure_ascii=False).lower()
        for forbidden in ("phenology", "autumn", "黄叶", "物候", "旅游", "worth_going"):
            self.assertNotIn(forbidden, text)

    def test_history_forward_is_written_to_latest_and_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            history_forward = {"module": "history_forward", "status": "OK", "points": {}, "regions": {}}
            with patch.object(pipeline, "LATEST_DIR", root / "latest"), patch.object(pipeline, "ARCHIVE_DIR", root / "archive"):
                pipeline.write_outputs(
                    now_local=datetime(2026, 9, 2, 10, tzinfo=pipeline.LOCAL_TZ),
                    status={},
                    hres={},
                    history={},
                    history_forward=history_forward,
                    ensemble={},
                    gfs={},
                    single_runs={},
                    spatial={},
                    long_range={},
                    summary={},
                    grid_registry={"module": "grid_registry"},
                    phenology_weather_summary={"module": "phenology_weather_summary"},
                )
            self.assertTrue((root / "latest" / "history_forward.json").is_file())
            self.assertTrue((root / "archive" / "2026-09-02" / "history_forward.json").is_file())
            self.assertTrue((root / "archive" / "2026-09-02" / "raw" / "history_forward.json.gz").is_file())
            self.assertTrue((root / "latest" / "grid_registry.json").is_file())
            self.assertTrue((root / "latest" / "phenology_weather_summary.json").is_file())
            self.assertTrue((root / "archive" / "2026-09-02" / "phenology_weather_summary.json").is_file())

    def test_failed_module_is_explicit_in_status(self):
        config = pipeline.load_config()
        modules = {name: {"status": "FAILED"} for name in ("hres", "history", "ensemble", "gfs", "single_runs")}
        status = pipeline.build_status(config, "2026-09-02T00:00:00Z", "2026-09-01", modules)
        self.assertEqual(status["pipeline_status"], "FAILED")
        self.assertEqual(status["modules"]["ensemble"], "FAILED")

    def test_stable_summary_schema_contract(self):
        schema_path = ROOT / "schemas" / "summary.schema.json"
        with schema_path.open(encoding="utf-8") as handle:
            schema = json.load(handle)
        self.assertEqual(schema["properties"]["schema_version"]["const"], pipeline.SCHEMA_VERSION)
        required = set(schema["required"])
        self.assertTrue({"data_date", "regions", "interpretation_boundary"}.issubset(required))
        self.assertIn("history_years", schema["properties"])
        region_required = set(schema["properties"]["regions"]["additionalProperties"]["required"])
        self.assertIn("weather_driver_vs_2025", region_required)
        self.assertIn("weather_driver_vs_2023", schema["properties"]["regions"]["additionalProperties"]["properties"])
        self.assertIn("weather_driver_vs_2024", schema["properties"]["regions"]["additionalProperties"]["properties"])

        compact_schema_path = ROOT / "schemas" / "phenology_weather_summary.schema.json"
        with compact_schema_path.open(encoding="utf-8") as handle:
            compact_schema = json.load(handle)
        self.assertIn("jiuzhaigou_aggregation", compact_schema["properties"]["model_policy"].get("properties", {}))
        light_window_properties = compact_schema["$defs"]["light_window"]["properties"]
        self.assertIn("usable_for_main_chain", light_window_properties)
        self.assertIn("usable_for_trend_reference", light_window_properties)
        self.assertIn("availability_note", light_window_properties)

    def test_zero_values_are_not_treated_as_missing_in_risk_checks(self):
        wet_snow_day = self.make_day("2026-09-01", 0)
        wet_snow_day["snowfall_cm"] = 0.2
        wet_snow_day["precipitation_mm"] = 1.0
        risk = pipeline.leaf_loss_weather_risk([wet_snow_day], date(2026, 9, 1))
        self.assertIn("wet_snow", risk["drivers"])
        self.assertIn("rain_snow", risk["drivers"])

        hres_record = {"status": "PASS", "daily": [self.make_day("2026-09-01", 0)]}
        gfs_record = {"status": "PASS", "daily": [self.make_day("2026-09-01", 0)]}
        crosscheck = pipeline.gfs_crosscheck(hres_record, gfs_record)
        self.assertEqual(crosscheck["cold_window_agreement"], "AGREE")

    def test_long_range_current_model_contract(self):
        self.assertEqual(pipeline.LONG_RANGE_MODEL_ID, "ncep_gefs05")
        self.assertEqual(pipeline.LONG_RANGE_ENSEMBLE_MEMBERS, 31)
        # `ncep_gefs05` documents about 35 days; requesting 36 sat one day past
        # what the daily run time can receive and produced a permanent PARTIAL.
        self.assertEqual(pipeline.LONG_RANGE_REQUESTED_FORECAST_DAYS, 35)
        self.assertEqual(pipeline.GEFS_LONG_FORECAST_DAYS, 35)
        self.assertEqual(pipeline.LONG_RANGE_REQUIRED_LEAD_END + 1, 34)
        self.assertEqual(
            pipeline.OPEN_METEO_ENDPOINTS["ensemble"],
            "https://ensemble-api.open-meteo.com/v1/ensemble",
        )

    def test_long_range_member_count_and_array_consistency(self):
        hourly = self.make_long_range_hourly()
        valid, check = pipeline.validate_long_range_members({"hourly": hourly})
        self.assertTrue(valid)
        self.assertEqual(check["actual_member_counts_by_variable"]["temperature_2m"], 31)

        del hourly["snowfall_member30"]
        valid, check = pipeline.validate_long_range_members({"hourly": hourly})
        self.assertFalse(valid)
        self.assertIn("snowfall:expected_31_got_30", check["missing_or_wrong_count"])

        hourly = self.make_long_range_hourly()
        hourly["wind_gusts_10m_member01"] = hourly["wind_gusts_10m_member01"][:-1]
        valid, check = pipeline.validate_long_range_members({"hourly": hourly})
        self.assertFalse(valid)
        self.assertIn("wind_gusts_10m_member01", check["array_length_mismatch"])

        hourly = self.make_long_range_hourly()
        for key in list(hourly):
            if key != "time":
                hourly[key][-1] = None
        valid, check = pipeline.validate_long_range_members({"hourly": hourly})
        self.assertTrue(valid)
        self.assertIn("temperature_2m", check["edge_truncated_variables"])
        self.assertEqual(check["variable_availability"]["temperature_2m"]["last_complete_index"], len(hourly["time"]) - 2)

    def test_long_range_horizon_and_three_day_windows(self):
        hourly = self.make_long_range_hourly()
        origin, daily_by_lead = pipeline.long_range_daily_member_values(hourly)
        self.assertEqual(origin, date(2026, 9, 2))
        horizon = pipeline.long_range_horizon_check(daily_by_lead)
        self.assertEqual(horizon["status"], "PASS")
        self.assertEqual(horizon["actual_lead_days"], 36)
        self.assertEqual(pipeline.long_range_window_definitions()[-1], (34, 35))
        windows = pipeline.build_long_range_windows(origin, daily_by_lead, {})
        self.assertEqual(len(windows), 7)
        self.assertEqual(windows[0]["horizon_class"], "D16_D18")
        self.assertEqual(windows[-1]["horizon_class"], "D34_D35")
        self.assertNotIn("hourly", windows[0])
        partial_windows = pipeline.build_long_range_windows(origin, {lead: values for lead, values in daily_by_lead.items() if lead <= 34}, {})
        partial_windows = pipeline.apply_signal_evolution("lixiaolu", partial_windows, [])
        self.assertEqual(partial_windows[-1]["status"], "UNAVAILABLE")
        self.assertEqual(partial_windows[-1]["signal_evolution"]["status"], "INSUFFICIENT_HISTORY")
        partial_horizon = pipeline.long_range_horizon_check({lead: values for lead, values in daily_by_lead.items() if lead <= 34})
        # Lead day 35 belongs to the trailing 3-day block and is published only
        # by the freshest long run, so losing it is an edge shortfall, not a
        # horizon failure.
        self.assertEqual(partial_horizon["status"], "PASS")
        self.assertEqual(partial_horizon["missing_lead_days"], [35])
        self.assertEqual(partial_horizon["edge_shortfall_lead_days"], [35])
        self.assertEqual(partial_horizon["missing_required_lead_days"], [])

    def test_long_range_optional_variable_edge_missing_is_undetermined(self):
        hourly = self.make_long_range_hourly()
        for key in [key for key in hourly if key.startswith("precipitation")]:
            hourly[key][-4:] = [None] * 4
        origin, daily_by_lead = pipeline.long_range_daily_member_values(hourly)
        windows = pipeline.build_long_range_windows(origin, daily_by_lead, {})
        last_window = windows[-1]
        self.assertEqual(last_window["status"], "OK")
        self.assertEqual(last_window["precipitation_background"]["signal"], "UNDETERMINED")
        self.assertIsNone(last_window["precipitation_background"]["member_support"])
        self.assertIn(last_window["forecast_uncertainty"], ("HIGH", "VERY_HIGH"))

    def test_long_range_percentiles_thresholds_and_coarse_qa(self):
        self.assertEqual(pipeline.percentile([1, 2, 3, 4], 0.5), 2.5)
        current = {f"member{index:02d}": {
            "temperature_mean_c": 4 + index * 0.1,
            "temperature_min_c": 3,
            "precipitation_mm": 1,
            "snowfall_cm": 0.2,
            "wind_gust_max_kmh": 55,
        } for index in range(31)}
        previous = {key: {**value, "temperature_mean_c": value["temperature_mean_c"] + 3} for key, value in current.items()}
        signal, diagnostics = pipeline.long_range_cold_window_signal(current, previous, "2026-09-18", "2026-09-20")
        self.assertEqual(signal["signal"], "STRONG")
        self.assertEqual(diagnostics["member_count"], 31)
        stats = pipeline.long_range_temperature_stats(current)
        self.assertEqual(stats["median"], 5.5)
        self.assertIn("interquartile_spread", stats)

        point = {"latitude": 48.69583, "longitude": 86.78382}
        coarse = self.make_payload()
        coarse["latitude"] = 48.5
        coarse["longitude"] = 87.0
        coarse["model"] = pipeline.LONG_RANGE_MODEL
        result = pipeline.validate_payload(
            coarse,
            point,
            pipeline.LONG_RANGE_MODEL,
            ["temperature_2m", "precipitation"],
            pipeline.LONG_RANGE_GRID_QA_LIMIT_KM,
        )
        self.assertTrue(result["valid"])
        self.assertEqual(pipeline.LONG_RANGE_GRID_QA_LIMIT_KM, 35.0)

    def test_long_range_uncertainty_and_run_persistence(self):
        uncertainty, drivers = pipeline.long_range_uncertainty({"spread": 12}, [0.5, 0.2], 30)
        self.assertEqual(uncertainty, "VERY_HIGH")
        self.assertIn("longer_lead_time", drivers)

        windows = [{
            "horizon_class": "D16_D18",
            "start_date": "2026-09-18",
            "end_date": "2026-09-20",
            "cold_window_signal": {"signal": "MODERATE", "persistence_runs": 0},
            "forecast_uncertainty": "MODERATE",
            "uncertainty_drivers": [],
        }]
        snapshots = [
            {"generated_at": "2026-09-01T00:00:00Z", "regions": {"lixiaolu": {"windows": [{"horizon_class": "D16_D18", "start_date": "2026-09-18", "cold_window_signal": {"signal": "MODERATE"}}]}}},
            {"generated_at": "2026-08-31T00:00:00Z", "regions": {"lixiaolu": {"windows": [{"horizon_class": "D16_D18", "start_date": "2026-09-18", "cold_window_signal": {"signal": "MODERATE"}}]}}},
            {"generated_at": "2026-08-30T00:00:00Z", "regions": {"lixiaolu": {"windows": [{"horizon_class": "D16_D18", "start_date": "2026-09-18", "cold_window_signal": {"signal": "MODERATE"}}]}}},
        ]
        updated = pipeline.apply_signal_evolution("lixiaolu", windows, snapshots)
        self.assertEqual(updated[0]["signal_evolution"]["status"], "PERSISTENT")
        self.assertEqual(updated[0]["signal_evolution"]["runs_seen"], 4)
        self.assertEqual(updated[0]["cold_window_signal"]["persistence_runs"], 4)

    def test_long_range_provisional_and_summary_boundaries(self):
        config = pipeline.load_config()
        active = pipeline.active_points(config)
        # The Siguniang scenic subregions and both Jiuzhaigou high-altitude
        # windows must stay inside the main chain.
        self.assertIn("SQG_SHUANGQIAO", active)
        self.assertIn("BPG_CENTER", active)
        self.assertIn("JZG_LONGHAI", active)
        self.assertEqual(pipeline.excluded_points(config), {})
        # A PROVISIONAL candidate point is excluded from the main chain.
        patched = copy.deepcopy(config)
        patched["points"]["JZG_TEST_PROVISIONAL"] = {
            "name": "九寨沟·候选高海拔点",
            "region": "jiuzhaigou",
            "subregion": "rize",
            "role": "route_high",
            "status": "PROVISIONAL",
            "latitude": 33.42,
            "longitude": 103.87,
        }
        self.assertFalse(pipeline.excluded_points(patched)["JZG_TEST_PROVISIONAL"]["usable_for_main_chain"])
        unavailable = pipeline.long_range_summary_for_chatgpt({
            "status": "UNAVAILABLE",
            "reason": "NO_VERIFIED_CORE_POINT",
        })
        self.assertEqual(unavailable, {
            "status": "UNAVAILABLE",
            "reason": "NO_VERIFIED_CORE_POINT",
        })

    def test_long_range_failure_keeps_short_chain_available(self):
        config = pipeline.load_config()
        modules = {
            name: {"status": "OK"}
            for name in ("hres", "history", "ensemble", "gfs", "single_runs", "spatial_sampling")
        }
        modules["long_range"] = {"status": "FAILED", "error": "OPEN_METEO_LONG_RANGE_NOT_AVAILABLE"}
        status = pipeline.build_status(config, "2026-09-02T00:00:00Z", "2026-09-01", modules)
        self.assertEqual(status["pipeline_status"], "PARTIAL")
        self.assertEqual(status["modules"]["hres"], "OK")
        self.assertEqual(status["modules"]["long_range"], "FAILED")

    def test_long_range_public_artifact_omits_raw_members(self):
        artifact = pipeline.public_long_range_artifact({
            "module": "long_range_background",
            "raw_points": {"SQG_SHUANGQIAO": {"hourly": {"time": ["x"]}}},
            "raw_references": {"lixiaolu": {"hourly": {"time": ["x"]}}},
            "regions": {},
        })
        self.assertNotIn("raw_points", artifact)
        self.assertNotIn("raw_references", artifact)
        self.assertFalse(artifact["raw_hourly_included"])

    def test_schema_is_additive_v120_and_long_range_contract_exists(self):
        with (ROOT / "schemas" / "summary.schema.json").open(encoding="utf-8") as handle:
            summary_schema = json.load(handle)
        with (ROOT / "schemas" / "long_range.schema.json").open(encoding="utf-8") as handle:
            long_range_schema = json.load(handle)
        self.assertEqual(summary_schema["properties"]["schema_version"]["const"], pipeline.SCHEMA_VERSION)
        region_required = set(summary_schema["properties"]["regions"]["additionalProperties"]["required"])
        self.assertTrue({"forecast_0_7d", "forecast_8_15d", "forecast_16_35d"}.issubset(region_required))
        self.assertEqual(long_range_schema["properties"]["model_id"]["const"], "ncep_gefs05")
        self.assertEqual(long_range_schema["properties"]["expected_ensemble_members"]["const"], 31)
        with (ROOT / "schemas" / "gefs.schema.json").open(encoding="utf-8") as handle:
            gefs_schema = json.load(handle)
        self.assertEqual(gefs_schema["properties"]["schema_version"]["const"], pipeline.SCHEMA_VERSION)
        self.assertEqual(gefs_schema["properties"]["members_total"]["const"], 31)
        self.assertEqual(gefs_schema["properties"]["near_range_model_id"]["const"], "ncep_gefs025")
        self.assertEqual(gefs_schema["properties"]["long_range_model_id"]["const"], "ncep_gefs05")

    def test_ecmwf_ensemble_requests_official_15_day_horizon(self):
        config = pipeline.load_config()
        requests = []
        record = self.make_ecmwf_ensemble_record()

        def fake_fetch(_client, **kwargs):
            requests.append(kwargs["params"].copy())
            return copy.deepcopy(record)

        with patch.object(pipeline, "fetch_point", side_effect=fake_fetch):
            module = pipeline.run_ensemble(
                config,
                object(),
                "2026-09-23T00:00:00Z",
                "2026-09-22",
            )

        self.assertEqual(pipeline.ECMWF_ENSEMBLE_FORECAST_DAYS, 15)
        self.assertEqual(module["status"], "OK")
        self.assertEqual(module["requested_forecast_days"], 15)
        self.assertEqual(len(requests), len(pipeline.core_region_ids(config)))
        self.assertTrue(all(item["models"] == "ecmwf_ifs025_ensemble" for item in requests))
        self.assertTrue(all(item["forecast_days"] == 15 for item in requests))
        self.assertTrue(all(item["timezone"] == "Asia/Shanghai" for item in requests))
        self.assertTrue(all(item["cell_selection"] == "nearest" for item in requests))
        self.assertTrue(all(item["elevation"] == "nan" for item in requests))
        k1_daily = module["points"]["JZG_TREESHENG"]["ensemble"]["distributions"]["daily_mean"]
        k1_dates = {item["date"] for item in k1_daily}
        self.assertEqual(len(k1_dates), 15)
        self.assertIn("2026-09-23", k1_dates)
        self.assertIn("2026-10-07", k1_dates)
        self.assertEqual(module["points"]["JZG_TREESHENG"]["qa"]["ensemble_member_check"]["status"], "PASS")

    def test_ecmwf_ensemble_d8_d15_reaches_brief_but_cutoff_keeps_post_nov1_out(self):
        config = pipeline.load_config()
        # A 15-day ensemble anchored on 2026-10-18 covers the whole
        # 10-24..11-01 target window, including its 8th..15th days.
        record_start = date(2026, 10, 18)
        full_record = self.make_ecmwf_ensemble_record(start_date=record_start)
        ensemble = {
            "model": "ECMWF IFS 0.25° Ensemble",
            "model_id": "ecmwf_ifs025_ensemble",
            "total_members": 51,
            "points": {
                "SQG_SHUANGQIAO": full_record,
                "JZG_TREESHENG": full_record,
            },
        }
        brief = pipeline.build_target_window_brief(
            config,
            record_start,
            {"points": {}},
            {"points": {}},
            ensemble,
            {"points": {}},
        )

        for date_key, point_id in (
            ("2026-10-25", "SQG_SHUANGQIAO"),
            ("2026-11-01", "JZG_TREESHENG"),
        ):
            ec_view = brief["days"][date_key][point_id]["morning"]["ec_ens"]
            self.assertTrue(ec_view["available"])
            self.assertEqual(ec_view["members_valid"], 51)
            self.assertIn(ec_view["status"], {"OK", "PARTIAL"})
            self.assertEqual(
                brief["days"][date_key][point_id]["morning"]["ensemble_agreement"],
                "ONE_ENSEMBLE_ONLY",
            )

        self.assertEqual(
            brief["dates"],
            [f"2026-10-{day:02d}" for day in range(24, 32)] + ["2026-11-01"],
        )
        self.assertNotIn("2026-11-02", json.dumps(brief, ensure_ascii=False))
        # The 11-01 cutoff keeps anything past it out of the brief horizon.
        self.assertEqual(
            pipeline._ensemble_window_view(
                full_record,
                date(2026, 11, 2),
                "MORNING",
                date(2026, 11, 1),
            )["reason"],
            "OUTSIDE_ECMWF_ENSEMBLE_HORIZON",
        )
        # A short (7-day) horizon ends on 10-24, so the tail of the target
        # window stays outside the ensemble horizon.
        short_record = self.make_ecmwf_ensemble_record(start_date=record_start, days=7)
        self.assertEqual(
            pipeline._ensemble_window_view(
                short_record,
                date(2026, 10, 27),
                "MORNING",
                date(2026, 11, 1),
            )["reason"],
            "OUTSIDE_ECMWF_ENSEMBLE_HORIZON",
        )

    def make_gefs_hourly(self, days=6, step_hours=3):
        start = datetime(2026, 9, 28)
        times = []
        for offset in range(0, days * 24, step_hours):
            times.append((start + timedelta(hours=offset)).strftime("%Y-%m-%dT%H:%M"))
        hourly = {"time": times}
        for variable in pipeline.GEFS_CORE_VARIABLES:
            base_values = []
            for index, _ in enumerate(times):
                day = index * step_hours // 24
                hour = (index * step_hours) % 24
                if variable == "temperature_2m":
                    value = 8 - (4 if day == 3 else 0) + (hour - 12) * 0.05
                elif variable == "precipitation":
                    value = 1.2 if day == 2 else 0.0
                elif variable == "snowfall":
                    value = 0.8 if day == 3 else 0.0
                elif variable == "cloud_cover":
                    value = 90.0 if day in (1, 2) else 10.0
                elif variable == "cloud_cover_low":
                    value = 70.0 if day == 2 else 5.0
                elif variable == "wind_gusts_10m":
                    value = 55.0 if day == 2 else 20.0
                elif variable == "wind_speed_10m":
                    value = 15.0
                else:
                    value = 60.0
                base_values.append(value)
            hourly[variable] = base_values
            for member in range(1, pipeline.GEFS_ENSEMBLE_MEMBERS):
                suffix = f"_member{member:02d}"
                hourly[f"{variable}{suffix}"] = [value + member * 0.01 for value in base_values]
        return hourly

    def test_gefs_model_member_and_distribution_contract(self):
        self.assertEqual(pipeline.GEFS_NEAR_MODEL_ID, "ncep_gefs025")
        self.assertEqual(pipeline.GEFS_LONG_MODEL_ID, "ncep_gefs05")
        self.assertEqual(pipeline.GEFS_ENSEMBLE_MEMBERS, 31)
        hourly = self.make_gefs_hourly()
        valid, check = pipeline._gefs_member_check(hourly)
        self.assertTrue(valid)
        self.assertEqual(check["members_valid"], 31)
        summary = pipeline._gefs_distribution_summary(
            [{"temperature_mean_c": float(value), "temperature_min_c": float(value), "temperature_max_c": float(value), "precipitation_mm": 0.0, "snowfall_cm": 0.0, "cloud_cover_pct": 20.0, "cloud_cover_low_pct": 5.0, "wind_speed_kmh": 5.0, "wind_gust_kmh": 10.0} for value in range(31)],
            31,
        )
        self.assertEqual(summary["probabilities"]["gust_gt_50kmh"]["members_valid"], 31)
        self.assertLessEqual(summary["temperature_2m"]["p10"], summary["temperature_2m"]["p90"])

    def test_gefs_optional_missing_does_not_invalidate_core_segment(self):
        hourly = self.make_gefs_hourly()
        for variable in ("cloud_cover_low", "cloud_cover_mid", "cloud_cover_high"):
            for key in list(hourly):
                if key == variable or key.startswith(f"{variable}_member"):
                    hourly.pop(key)
        valid, check = pipeline._gefs_member_check(hourly)
        self.assertTrue(valid)
        self.assertEqual(check["status"], "PASS")
        self.assertEqual(check["required_missing_variables"], [])
        self.assertEqual(
            set(check["optional_missing_variables"]),
            {"cloud_cover_low", "cloud_cover_mid", "cloud_cover_high"},
        )
        segment = pipeline._build_gefs_segment(
            {"hourly": hourly, "response": {"timezone": "Asia/Shanghai", "utc_offset_seconds": 28800}},
            "long_range", "ncep_gefs05", "GFS Ensemble 0.5°", "0.5° (~50 km)", date(2026, 10, 6),
        )
        self.assertEqual(segment["status"], "OK")

    def test_gefs_required_missing_is_partial_and_all_core_missing_is_failed(self):
        hourly = self.make_gefs_hourly()
        for key in list(hourly):
            if key == "precipitation" or key.startswith("precipitation_member"):
                hourly.pop(key)
        valid, check = pipeline._gefs_member_check(hourly)
        self.assertTrue(valid)
        self.assertEqual(check["status"], "PARTIAL")
        self.assertEqual(check["required_missing_variables"], ["precipitation"])
        segment = pipeline._build_gefs_segment(
            {"hourly": hourly, "response": {"timezone": "Asia/Shanghai", "utc_offset_seconds": 28800}},
            "long_range", "ncep_gefs05", "GFS Ensemble 0.5°", "0.5° (~50 km)", date(2026, 10, 6),
        )
        self.assertEqual(segment["status"], "PARTIAL")
        all_core_missing = self.make_gefs_hourly()
        for variable in pipeline.GEFS_CORE_VARIABLES:
            for key in list(all_core_missing):
                if key == variable or key.startswith(f"{variable}_member"):
                    all_core_missing.pop(key)
        valid, check = pipeline._gefs_member_check(all_core_missing)
        self.assertFalse(valid)
        self.assertEqual(check["status"], "FAIL")
        failed_segment = pipeline._build_gefs_segment(
            {"hourly": all_core_missing, "response": {"timezone": "Asia/Shanghai", "utc_offset_seconds": 28800}},
            "long_range", "ncep_gefs05", "GFS Ensemble 0.5°", "0.5° (~50 km)", date(2026, 10, 6),
        )
        self.assertEqual(failed_segment["status"], "FAILED")

    def test_gefs_point_status_counts_distinguish_ok_partial_failed_and_usable(self):
        config = pipeline.load_config()
        hourly = self.make_gefs_hourly()

        def fake_fetch(*args, **kwargs):
            return {
                "status": "PASS",
                "response": {"timezone": "Asia/Shanghai", "utc_offset_seconds": 28800},
                "hourly": copy.deepcopy(hourly),
                "gefs_missing_variables": [],
            }

        with tempfile.TemporaryDirectory() as directory, patch.object(pipeline, "_fetch_gefs_segment", side_effect=fake_fetch):
            result = pipeline.run_gefs(
                config,
                object(),
                "2026-09-22T00:00:00Z",
                "2026-09-21",
                cache_dir=Path(directory),
            )
        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["successful_points"], len(pipeline.active_points(config)))
        self.assertEqual(result["partial_points"], 0)
        self.assertEqual(result["failed_points"], 0)
        self.assertEqual(result["usable_points"], len(pipeline.active_points(config)))

    def test_gefs_partial_member_uses_members_valid_denominator(self):
        hourly = self.make_gefs_hourly()
        for variable in pipeline.GEFS_CORE_VARIABLES:
            hourly.pop(f"{variable}_member30")
        valid, check = pipeline._gefs_member_check(hourly)
        self.assertTrue(valid)
        self.assertEqual(check["status"], "PARTIAL")
        self.assertEqual(check["members_valid"], 30)
        probability = pipeline._gefs_probability([1.0] * 15, lambda value: value > 0.5, 30)
        self.assertEqual(probability["members_valid"], 30)
        self.assertEqual(probability["probability"], 0.5)
        unavailable = pipeline._gefs_probability([], lambda value: value > 0.5, 30)
        self.assertIsNone(unavailable["probability"])

    def test_gefs_event_phase_concentrated_distribution(self):
        hourly = self.make_gefs_hourly()
        segment = pipeline._build_gefs_segment(
            {"hourly": hourly, "response": {"timezone": "Asia/Shanghai", "utc_offset_seconds": 28800}},
            "long_range", "ncep_gefs05", "GFS Ensemble 0.5°", "0.5° (~50 km)", date(2026, 10, 6),
        )
        cloud_phase = segment["event_phases"]["CLOUD_EVENT"]
        self.assertEqual(cloud_phase["status"], "SIGNAL")
        self.assertEqual(cloud_phase["members_with_event"], 31)
        self.assertIn("2026-09-29", cloud_phase["event_day_distribution"])
        self.assertIn(cloud_phase["phase_confidence"], {"HIGH", "MEDIUM", "LOW"})

    def test_gefs_event_phase_multimodal_and_no_signal(self):
        local = pipeline.LOCAL_TZ
        events = [
            {"event_start": datetime(2026, 10, 2, 0, tzinfo=local), "event_peak": datetime(2026, 10, 2, 6, tzinfo=local), "event_end": datetime(2026, 10, 2, 12, tzinfo=local)},
            {"event_start": datetime(2026, 10, 4, 0, tzinfo=local), "event_peak": datetime(2026, 10, 4, 6, tzinfo=local), "event_end": datetime(2026, 10, 4, 12, tzinfo=local)},
        ]
        phase = pipeline._gefs_event_phase("CLOUD_EVENT", events, 2)
        self.assertTrue(phase["multimodal"])
        self.assertEqual(phase["phase_confidence"], "LOW")
        no_signal = pipeline._gefs_event_phase("SNOW_EVENT", [], 31)
        self.assertEqual(no_signal["status"], "NO_SIGNAL")
        self.assertEqual(no_signal["event_day_distribution"]["none"]["members"], 31)

    def test_gefs_cutoff_and_window_boundaries(self):
        times = ["2026-10-06T18:00", "2026-10-07T00:00", "2026-10-07T06:00"]
        self.assertEqual(
            pipeline._gefs_window_indices(times, date(2026, 10, 6), "NIGHT", date(2026, 10, 6)),
            [0],
        )
        self.assertNotIn("2026-11-02", [date.fromisoformat(key) for key in pipeline._gefs_time_groups(times, date(2026, 10, 6))])

    def test_gefs_cache_round_trip_and_stale_identity(self):
        point = {"id": "SQG_SHUANGQIAO", **pipeline.load_config()["points"]["SQG_SHUANGQIAO"]}
        record = {"response": {"retrieval_time": "2026-09-22T00:00:00Z"}, "hourly": {"time": []}}
        with tempfile.TemporaryDirectory() as directory:
            cache_dir = Path(directory)
            pipeline._write_gefs_cache(point, "ncep_gefs05", pipeline.GEFS_CORE_VARIABLES, record, cache_dir)
            self.assertIsNotNone(pipeline._load_gefs_cache(point, "ncep_gefs05", pipeline.GEFS_CORE_VARIABLES, cache_dir))
            changed_point = copy.deepcopy(point)
            changed_point["latitude"] += 0.01
            self.assertIsNone(pipeline._load_gefs_cache(changed_point, "ncep_gefs05", pipeline.GEFS_CORE_VARIABLES, cache_dir))

    def test_gefs_optional_capability_probe_is_once_per_model_per_run(self):
        point = {"id": "SQG_SHUANGQIAO", **pipeline.load_config()["points"]["SQG_SHUANGQIAO"]}
        record = {
            "status": "PASS",
            "response": {"retrieval_time": "2026-09-22T00:00:00Z"},
            "hourly": self.make_gefs_hourly(),
        }
        with tempfile.TemporaryDirectory() as directory, patch.object(
            pipeline, "fetch_point", return_value=copy.deepcopy(record)
        ), patch.object(pipeline, "_fetch_gefs_optional_solar") as probe:
            capabilities = {}
            for _ in range(2):
                pipeline._fetch_gefs_segment(
                    pipeline.load_config(),
                    point,
                    object(),
                    "long_range",
                    "ncep_gefs05",
                    "GFS Ensemble 0.5°",
                    "0.5° (~50 km)",
                    35,
                    "2026-09-22T00:00:00Z",
                    date(2026, 10, 6),
                    cache_dir=Path(directory),
                    solar_capabilities=capabilities,
                )
            self.assertEqual(probe.call_count, 1)
            self.assertFalse(capabilities["ncep_gefs05"])

    def test_gefs_deterministic_outlier_and_ensemble_consensus(self):
        gefs_window = {
            "status": "OK",
            "statistics": {
                "cloud_cover": {"p10": 0, "p25": 10, "p75": 20, "p90": 40},
                "cloud_cover_low": {"p10": 0, "p25": 10, "p75": 20, "p90": 40},
                "temperature_2m": {"p10": 0, "p25": 2, "p75": 4, "p90": 6},
                "precipitation": {"p10": 0, "p25": 0, "p75": 1, "p90": 3},
                "snowfall": {"p10": 0, "p25": 0, "p75": 1, "p90": 3},
                "probabilities": {
                    "cloud_cover_gt_70pct": {"probability": 0.1},
                    "precipitation_gt_0_5mm": {"probability": 0.1},
                    "snowfall_gt_0_5cm": {"probability": 0.1},
                },
            },
        }
        gfs_window = {
            "status": "OK",
            "total_cloud_pct": 100,
            "low_cloud_pct": 80,
            "temperature_mean_c": 30,
            "precipitation_mm": 0,
            "snowfall_cm": 0,
        }
        consistency = pipeline._deterministic_consistency(gfs_window, gefs_window)
        self.assertTrue(consistency["deterministic_outlier"])
        self.assertEqual(consistency["cloud_cover"], "OUTLIER")
        ec = copy.deepcopy(gefs_window)
        ec["statistics"]["probabilities"]["cloud_cover_gt_70pct"]["probability"] = 0.1
        self.assertEqual(pipeline._ensemble_consensus(ec, gefs_window)["agreement"], "HIGH")
        gefs_window["statistics"]["probabilities"]["cloud_cover_gt_70pct"]["probability"] = 0.9
        self.assertEqual(pipeline._ensemble_consensus(ec, gefs_window)["agreement"], "LOW")
        self.assertEqual(
            pipeline._ensemble_consensus({"status": "UNAVAILABLE"}, ec)["agreement"],
            "ONE_ENSEMBLE_ONLY",
        )
        self.assertEqual(
            pipeline._ensemble_consensus({"status": "UNAVAILABLE"}, {"status": "UNAVAILABLE"})["agreement"],
            "UNAVAILABLE",
        )
        self.assertEqual(
            pipeline._viewing_signal(ec, {"status": "UNAVAILABLE"}, {}, "ONE_ENSEMBLE_ONLY")["model_agreement"],
            "SINGLE_ENSEMBLE",
        )

    def test_gefs_night_window_and_non_itinerary_points_stay_out_of_brief(self):
        times = ["2026-10-05T18:00", "2026-10-06T07:00", "2026-10-06T18:00", "2026-10-07T07:00"]
        self.assertEqual(pipeline._gefs_window_indices(times, date(2026, 10, 5), "NIGHT", date(2026, 10, 6)), [0, 1])
        self.assertEqual(pipeline._gefs_window_indices(times, date(2026, 10, 6), "NIGHT", date(2026, 10, 6)), [2])
        brief = pipeline.build_target_window_brief(pipeline.load_config(), date(2026, 9, 22), {"points": {}}, {"points": {}}, {"points": {}}, {"points": {}})
        # Only the VERIFIED registry appears in the brief, and every point in
        # the brief is one the itinerary actually visits on that date.
        self.assertEqual(
            brief["verified_location_ids"],
            [
                "BPG_CENTER",
                "JZG_LONGHAI",
                "JZG_NORILANG",
                "JZG_PRIMEVAL",
                "JZG_TREESHENG",
                "LXL_HIGH_PASS",
                "SQG_SHUANGQIAO",
            ],
        )
        self.assertEqual(brief["excluded_location_ids"], [])
        location_text = json.dumps(brief["days"], ensure_ascii=False)
        for point_id in ("SQG_SHUANGQIAO", "BPG_CENTER", "LXL_HIGH_PASS", "JZG_TREESHENG", "JZG_NORILANG", "JZG_PRIMEVAL", "JZG_LONGHAI"):
            self.assertIn(point_id, location_text)
        self.assertNotIn("2026-10-23", location_text)
        self.assertNotIn("2026-11-02", location_text)

    def test_gefs_failure_keeps_existing_pipeline_partial(self):
        config = pipeline.load_config()
        modules = {name: {"status": "OK"} for name in ("hres", "history", "ensemble", "gfs", "single_runs", "spatial_sampling", "long_range", "history_forward", "weather_events")}
        modules["gefs"] = {"status": "FAILED"}
        status = pipeline.build_status(config, "2026-09-22T00:00:00Z", "2026-09-21", modules)
        self.assertEqual(status["pipeline_status"], "PARTIAL")
        self.assertEqual(status["modules"]["gefs"], "FAILED")
        self.assertEqual(status["modules"]["hres"], "OK")

    def test_target_window_brief_is_verified_only_and_cut_off(self):
        config = pipeline.load_config()
        empty = {"points": {}}
        gefs = {"members_total": 31, "points": {}}
        brief = pipeline.build_target_window_brief(config, date(2026, 9, 22), empty, empty, empty, gefs)
        self.assertEqual(
            brief["dates"],
            [f"2026-10-{day:02d}" for day in range(24, 32)] + ["2026-11-01"],
        )
        self.assertNotIn("2026-11-02", json.dumps(brief))
        self.assertIn("days", brief)
        self.assertNotIn("locations", brief)
        for items in brief["days"].values():
            self.assertTrue(all(item["usable_for_main_chain"] for item in items.values()))
        self.assertEqual(
            brief["itinerary_focus"]["2026-10-24"]["locations"],
            ["SQG_SHUANGQIAO", "BPG_CENTER", "LXL_HIGH_PASS"],
        )
        self.assertEqual(
            brief["itinerary_focus"]["2026-10-31"]["locations"],
            ["JZG_TREESHENG", "JZG_NORILANG", "JZG_PRIMEVAL", "JZG_LONGHAI"],
        )

    def test_target_window_brief_is_compact_and_does_not_embed_raw_ensemble_details(self):
        config = pipeline.load_config()
        empty = {"points": {}}
        brief = pipeline.build_target_window_brief(
            config, date(2026, 9, 22), empty, empty, empty, {"members_total": 31, "points": {}},
        )
        serialized = json.dumps(brief, ensure_ascii=False, separators=(",", ":"))
        self.assertLess(len(serialized), 250_000)
        self.assertNotIn("_member", serialized)
        self.assertNotIn("raw_points", serialized)
        self.assertNotIn("event_day_distribution", serialized)
        self.assertEqual(brief["window_overview"]["largest_model_disagreement_dates"], [])
        self.assertEqual(brief["window_overview"]["single_ensemble_only_dates"], [])

    def test_target_window_brief_lists_only_true_disagreement_dates(self):
        config = pipeline.load_config()
        empty = {"points": {}}

        def fake_location(config, point_id, target_date, forecast_date, hres, gfs, ensemble, gefs, cutoff_date):
            date_key = target_date.isoformat()
            agreement = "LOW" if date_key == "2026-10-31" else "ONE_ENSEMBLE_ONLY" if date_key == "2026-10-24" else "UNAVAILABLE"
            return {
                "location_id": point_id,
                "location_name": point_id,
                "usable_for_main_chain": True,
                "forecast_granularity": "trend_only",
                "daily": {
                    "ec_det": {"cloud": 10},
                    "gfs_det": {"cloud": 20},
                },
                "morning": {
                    "ensemble_agreement": agreement,
                    "viewing_conditions": {
                        "cloud_signal": "CLEAR",
                        "low_cloud_signal": "LOW",
                        "precip_signal": "LOW",
                        "wind_signal": "LOW",
                    },
                },
                "afternoon": {
                    "ensemble_agreement": agreement,
                    "viewing_conditions": {
                        "cloud_signal": "CLEAR",
                        "low_cloud_signal": "LOW",
                        "precip_signal": "LOW",
                        "wind_signal": "LOW",
                    },
                },
                "event_phase": {
                    "cold_window": {"relevant_to_date": False},
                    "precip_window": {"relevant_to_date": False},
                    "snow_window": {"relevant_to_date": False},
                },
            }

        with patch.object(pipeline, "_target_window_location", side_effect=fake_location):
            brief = pipeline.build_target_window_brief(config, date(2026, 9, 22), empty, empty, empty, empty)
        overview = brief["window_overview"]
        self.assertEqual(overview["largest_model_disagreement_dates"], ["2026-10-31"])
        self.assertEqual(overview["single_ensemble_only_dates"], ["2026-10-24"])

    def test_weather_event_flags_window_metrics_and_mechanical_stress(self):
        day = self.make_day("2026-09-01", -6)
        day.update({"precipitation_mm": 2.0, "snowfall_cm": 0.5, "wind_gust_max_kmh": 70})
        event = pipeline.derive_weather_event_day(day, "finalized_history")
        for flag in (
            "freeze",
            "hard_freeze_le_minus5",
            "gust_ge_50",
            "gust_ge_65",
            "rain_day",
            "snow_day",
            "rain_and_gust_ge_50",
            "snow_and_gust_ge_50",
            "freeze_and_snow",
        ):
            self.assertTrue(event[flag], flag)
        self.assertEqual(event["mechanical_leaf_stress"]["level"], "HIGH")
        self.assertIn("very_strong_wind", event["mechanical_leaf_stress"]["reasons"])
        metrics = pipeline.weather_event_window_metrics([event])
        self.assertEqual(metrics["precipitation_days"], 1)
        self.assertEqual(metrics["snowfall_days"], 1)
        self.assertEqual(metrics["gust_ge_50_days"], 1)
        self.assertEqual(metrics["gust_ge_65_days"], 1)
        self.assertEqual(metrics["freeze_and_snow_days"], 1)
        self.assertEqual(metrics["wind_gust_max_kmh"], 70.0)
        self.assertEqual(metrics["mechanical_leaf_stress"]["level"], "HIGH")

    def test_weather_event_cache_backfill_hit_append_and_fingerprint_recompute(self):
        config = pipeline.load_config()
        point = pipeline.active_points(config)["SQG_SHUANGQIAO"]
        params = {
            "models": "ecmwf_ifs",
            "cell_selection": "nearest",
            "elevation": "nan",
            "timezone": "Asia/Shanghai",
            "start_date": "2025-09-01",
            "end_date": "2025-09-03",
        }
        record = self.make_history_forward_record(point, params)
        source_cache = pipeline.history_cache_from_record(
            config, point, 2025, record, params["start_date"], params["end_date"], mode="TEST"
        )
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            first, first_info = pipeline.update_weather_events_cache(
                config, point, 2025, source_cache, "2026-09-11T00:00:00Z", cache_dir=cache_dir
            )
            self.assertEqual(first_info["status"], "FILLED")
            self.assertEqual(first_info["cache_update"]["cache_fills"], 1)
            self.assertEqual(first_info["cache_update"]["dates_added"], 3)
            path = pipeline.weather_events_cache_path(config, 2025, "SQG_SHUANGQIAO", cache_dir)
            first_bytes = path.read_bytes()

            second, second_info = pipeline.update_weather_events_cache(
                config, point, 2025, source_cache, "2026-09-11T00:01:00Z", cache_dir=cache_dir
            )
            self.assertEqual(second_info["status"], "HIT")
            self.assertEqual(second_info["cache_update"]["cache_hits"], 1)
            self.assertEqual(path.read_bytes(), first_bytes)
            self.assertEqual(len(second["daily"]), 3)

            appended = copy.deepcopy(source_cache)
            appended["daily"].append(self.make_day("2025-09-04", 3))
            third, third_info = pipeline.update_weather_events_cache(
                config, point, 2025, appended, "2026-09-11T00:02:00Z", cache_dir=cache_dir
            )
            self.assertEqual(third_info["status"], "UPDATED")
            self.assertEqual(third_info["cache_update"]["dates_added"], 1)
            self.assertEqual(len(third["daily"]), 4)

            changed = copy.deepcopy(appended)
            changed["daily"][1]["wind_gust_max_kmh"] = 71
            fourth, fourth_info = pipeline.update_weather_events_cache(
                config, point, 2025, changed, "2026-09-11T00:03:00Z", cache_dir=cache_dir
            )
            self.assertEqual(fourth_info["status"], "UPDATED")
            self.assertEqual(fourth_info["cache_update"]["dates_recomputed"], 1)
            self.assertEqual(fourth_info["cache_update"]["source_dates_changed"], 1)
            self.assertEqual(fourth["daily"][1]["wind_gust_max_kmh"], 71.0)

    def test_weather_event_cache_identity_mismatch_is_invalid_without_api_fetch(self):
        config = pipeline.load_config()
        point = pipeline.active_points(config)["SQG_SHUANGQIAO"]
        params = {
            "models": "ecmwf_ifs",
            "cell_selection": "nearest",
            "elevation": "nan",
            "timezone": "Asia/Shanghai",
            "start_date": "2025-09-01",
            "end_date": "2025-09-01",
        }
        source_cache = pipeline.history_cache_from_record(
            config, point, 2025, self.make_history_forward_record(point, params), params["start_date"], params["end_date"], mode="TEST"
        )
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            pipeline.update_weather_events_cache(config, point, 2025, source_cache, "2026-09-11T00:00:00Z", cache_dir=cache_dir)
            event_path = pipeline.weather_events_cache_path(config, 2025, "SQG_SHUANGQIAO", cache_dir)
            event_cache = json.loads(event_path.read_text(encoding="utf-8"))
            event_cache["identity"]["timezone"] = "UTC"
            event_path.write_text(json.dumps(event_cache), encoding="utf-8")
            invalid, info = pipeline.update_weather_events_cache(
                config, point, 2025, source_cache, "2026-09-11T00:01:00Z", cache_dir=cache_dir
            )
        self.assertIsNone(invalid)
        self.assertEqual(info["status"], "INVALID")
        self.assertIn("timezone", info["identity_mismatches"])

    def test_weather_event_source_uses_unique_grid_max_gust_and_excludes_provisional(self):
        config = pipeline.load_config()

        def item(point_id, grid, temperature, gust):
            point = config["points"][point_id]
            day = self.make_day("2025-09-01", 2)
            day.update({"temperature_mean_c": temperature, "temperature_min_c": temperature - 3, "temperature_max_c": temperature + 3, "night_min_c": temperature - 4, "wind_gust_max_kmh": gust})
            event = pipeline.derive_weather_event_day(day, "finalized_history")
            return {
                "status": "OK",
                "identity": {
                    "requested_coordinate": {"latitude": point["latitude"], "longitude": point["longitude"]},
                    "returned_grid_coordinate": grid,
                    "returned_elevation": 1800,
                    "grid_distance_km": 2,
                    "grid_distance_limit_km": pipeline.HISTORY_GRID_QA_LIMIT_KM,
                    "grid_cell_key": f"{grid['latitude']},{grid['longitude']}",
                    "timezone": "Asia/Shanghai",
                    "model": pipeline.HISTORY_MODEL,
                    "endpoint": pipeline.OPEN_METEO_ENDPOINTS["history"],
                    "source": "Open-Meteo",
                },
                "qa": {"final_status": "PASS", "grid_distance_km": 2, "grid_distance_limit_km": pipeline.HISTORY_GRID_QA_LIMIT_KM},
                "daily": [event],
            }

        source = {
            "JZG_TREESHENG": item("JZG_TREESHENG", {"latitude": 48.75, "longitude": 87.0}, 10, 20),
            "JZG_NORILANG": item("JZG_NORILANG", {"latitude": 48.7500001, "longitude": 87.0}, 10, 30),
            "JZG_LONGHAI": item("JZG_LONGHAI", {"latitude": 48.5, "longitude": 87.0}, 0, 70),
        }
        result = pipeline.build_weather_event_source_summary(
            config, ["JZG_TREESHENG", "JZG_NORILANG", "JZG_LONGHAI"], source, source_state="finalized_history", minimum_verified_unique_grids=2
        )
        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["sampling"]["unique_model_grids"], 2)
        self.assertEqual(result["metrics"]["temperature_mean_c"], 5.0)
        self.assertEqual(result["metrics"]["wind_gust_max_kmh"], 70.0)
        self.assertEqual(result["metrics"]["max_gust_source_point_id"], "JZG_LONGHAI")
        self.assertEqual(result["metrics"]["max_gust_source_grid_cell_key"], "48.500000,87.000000")
        provisional = pipeline.build_weather_event_source_summary(
            config, ["K4", "K5", "K6"], {"K5": source["JZG_TREESHENG"]}, source_state="finalized_history"
        )
        self.assertEqual(provisional["sampling"]["excluded_point_ids"], ["K4", "K6"])
        self.assertNotIn("K4", provisional["sampling"]["valid_point_ids"])

    def test_cooling_episode_candidates_find_repeated_weather_cooling_and_ignore_small_noise(self):
        values = [10, 10, 10, 7, 6, 10, 10, 10, 7, 6, 10, 10, 10, 10, 7, 6, 10]
        days = []
        for offset, value in enumerate(values):
            day = self.make_day((date(2025, 9, 1) + timedelta(days=offset)).isoformat(), value - 2)
            day["temperature_mean_c"] = value
            day["temperature_min_c"] = value - 2
            day["temperature_max_c"] = value + 2
            day["night_min_c"] = value - 2
            days.append(pipeline.derive_weather_event_day(day, "finalized_history"))
        episodes = pipeline.cooling_episode_candidates(days)
        self.assertEqual(len(episodes), 3)
        self.assertEqual([item["start_date"] for item in episodes], ["2025-09-04", "2025-09-09", "2025-09-15"])
        self.assertTrue(all(item["temperature_drop_c"] >= 3 for item in episodes))

        noise = []
        for offset, value in enumerate([10, 9.5, 10.2, 9.8, 10.1, 9.7, 10]):
            day = self.make_day((date(2025, 9, 1) + timedelta(days=offset)).isoformat(), value - 2)
            day["temperature_mean_c"] = value
            day["night_min_c"] = value - 2
            noise.append(pipeline.derive_weather_event_day(day, "finalized_history"))
        self.assertEqual(pipeline.cooling_episode_candidates(noise), [])

    def test_weather_events_failure_is_partial_and_does_not_hide_hres(self):
        config = pipeline.load_config()
        modules = {name: {"status": "OK"} for name in ("hres", "history", "ensemble", "gfs", "single_runs", "spatial_sampling")}
        modules["history_forward"] = {"status": "OK"}
        modules["long_range"] = {"status": "OK"}
        modules["phenology_weather_summary"] = {"status": "OK"}
        modules["weather_events"] = {"status": "FAILED", "error": "TEST_FAILURE"}
        status = pipeline.build_status(config, "2026-09-11T00:00:00Z", "2026-09-10", modules)
        self.assertEqual(status["pipeline_status"], "PARTIAL")
        self.assertEqual(status["modules"]["hres"], "OK")
        self.assertEqual(status["modules"]["weather_events"], "FAILED")

    def test_weather_events_module_schema_is_valid(self):
        module = pipeline.module_header(
            "weather_events",
            "2026-09-11T00:00:00Z",
            "2026-09-10",
            "FAILED",
            source="Open-Meteo",
            finalized_history={"source_state": "finalized_history"},
            forecast={"source_state": "forecast", "historical_promotion_allowed": False},
            regions={},
            cooling_episode_candidates={},
            weather_event_cache={"historical_api_requests": 0},
            qa={"final_status": "FAILED"},
            interpretation_boundary="Weather events only.",
        )
        with (ROOT / "schemas" / "weather_events.schema.json").open(encoding="utf-8") as handle:
            schema = json.load(handle)
        self.assertEqual(list(Draft202012Validator(schema).iter_errors(module)), [])

    def test_long_range_model_id_mismatch_is_rejected(self):
        payload = self.make_payload(model=pipeline.LONG_RANGE_MODEL)
        payload["model_id"] = "wrong_model_id"
        result = pipeline.validate_payload(
            payload,
            {"latitude": 48.75, "longitude": 86.75},
            pipeline.LONG_RANGE_MODEL,
            ["temperature_2m", "precipitation"],
            1,
            accepted_model_values=(pipeline.LONG_RANGE_MODEL_ID, pipeline.LONG_RANGE_MODEL),
            accepted_model_ids=(pipeline.LONG_RANGE_MODEL_ID,),
        )
        self.assertFalse(result["valid"])
        self.assertIn("MODEL_ID_MISMATCH", result["reason"])

    # ------------------------------------------------------------------
    # Unified weather variable system (schema 1.4.0)
    # ------------------------------------------------------------------

    def make_unified_deterministic_record(self, *, start=datetime(2026, 9, 30), days=7):
        """A deterministic series carrying every unified variable.

        Layers carry deliberately distinct values so that any accidental
        total-minus-low derivation is immediately visible.
        """
        times = []
        for offset in range(days * 24):
            times.append((start + timedelta(hours=offset)).strftime("%Y-%m-%dT%H:%M"))
        hourly = {"time": times}
        for index, stamp in enumerate(times):
            hour = int(stamp[11:13])
            hourly.setdefault("temperature_2m", []).append(8.0 + hour * 0.1)
            hourly.setdefault("dew_point_2m", []).append(2.0 + hour * 0.05)
            hourly.setdefault("relative_humidity_2m", []).append(70.0)
            hourly.setdefault("precipitation", []).append(0.4 if hour == 15 else 0.0)
            hourly.setdefault("rain", []).append(0.3 if hour == 15 else 0.0)
            hourly.setdefault("snowfall", []).append(0.0)
            hourly.setdefault("cloud_cover", []).append(80.0)
            hourly.setdefault("cloud_cover_low", []).append(60.0)
            hourly.setdefault("cloud_cover_mid", []).append(30.0)
            hourly.setdefault("cloud_cover_high", []).append(10.0)
            hourly.setdefault("wind_speed_10m", []).append(12.0)
            hourly.setdefault("wind_direction_10m", []).append(350.0 if index % 2 == 0 else 10.0)
            hourly.setdefault("wind_gusts_10m", []).append(45.0)
            hourly.setdefault("sunshine_duration", []).append(1200.0)
        return {
            "status": "PASS",
            "source": "Open-Meteo",
            "response": {"timezone": "Asia/Shanghai", "utc_offset_seconds": 28800},
            "solar_variable": "sunshine_duration",
            "hourly": hourly,
        }

    def test_unified_variable_constants_keep_models_independent(self):
        # 1. The same vocabulary is requested from every forecast model.
        self.assertEqual(set(pipeline.HRES_VARIABLES), set(pipeline.GFS_VARIABLES))
        self.assertEqual(set(pipeline.HRES_VARIABLES), set(pipeline.ENSEMBLE_VARIABLES))
        self.assertEqual(set(pipeline.HRES_VARIABLES), set(pipeline.GEFS_CORE_VARIABLES))
        for variable in ("cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high"):
            self.assertIn(variable, pipeline.HRES_VARIABLES)
            self.assertIn(variable, pipeline.GFS_VARIABLES)
        # 2. Sustained wind and gust stay separate quantities.
        self.assertTrue(set(pipeline.GUST_VARIABLES).isdisjoint(pipeline.SUSTAINED_WIND_VARIABLES))
        self.assertEqual(pipeline.GUST_VARIABLES, ("wind_gusts_10m",))
        self.assertEqual(pipeline.SUSTAINED_WIND_VARIABLES, ("wind_speed_10m",))
        self.assertEqual(pipeline.GUST_THRESHOLD_LEVELS_KMH, (30.0, 40.0, 50.0, 60.0))
        # 3. Wind direction is never reduced with arithmetic percentiles.
        self.assertNotIn("wind_direction_10m", pipeline.ENSEMBLE_DISTRIBUTION_VARIABLES)
        self.assertEqual(pipeline.ECMWF_ENSEMBLE_FORECAST_DAYS, 15)

    def test_hres_parses_four_cloud_layers_from_api_values(self):
        record = self.make_unified_deterministic_record()
        window = pipeline._deterministic_hourly_window(
            record, date(2026, 9, 30), "AFTERNOON", date(2026, 10, 6)
        )
        self.assertEqual(window["status"], "OK")
        self.assertEqual(window["total_cloud_pct"], 80.0)
        self.assertEqual(window["low_cloud_pct"], 60.0)
        self.assertEqual(window["mid_cloud_pct"], 30.0)
        self.assertEqual(window["high_cloud_pct"], 10.0)
        # Mid/high cloud come from the API, never from total minus low.
        self.assertNotEqual(window["mid_cloud_pct"], window["total_cloud_pct"] - window["low_cloud_pct"])
        self.assertNotEqual(window["high_cloud_pct"], window["total_cloud_pct"] - window["low_cloud_pct"])
        compact = pipeline._compact_deterministic_view(window)
        self.assertEqual(compact["mid_cloud"], 30.0)
        self.assertEqual(compact["high_cloud"], 10.0)

    def test_gfs_module_publishes_four_cloud_layers_and_variable_status(self):
        config = pipeline.load_config()
        record = self.make_unified_deterministic_record()
        record["daily"] = [
            {"date": (date(2026, 9, 30) + timedelta(days=offset)).isoformat(), "complete": True}
            for offset in range(7)
        ]
        record["variable_status"] = pipeline.variable_status_classification(
            pipeline.variable_availability(record["hourly"], pipeline.GFS_VARIABLES),
            required_variables=pipeline.GFS_REQUIRED_VARIABLES,
            optional_variables=pipeline.GFS_OPTIONAL_VARIABLES,
        )
        with patch.object(pipeline, "fetch_point", return_value=copy.deepcopy(record)) as fetch:
            module = pipeline.run_gfs(config, object(), "2026-09-30T00:00:00Z", "2026-09-30")
        requested = fetch.call_args.kwargs["variables"]
        for variable in ("cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high"):
            self.assertIn(variable, requested)
        self.assertEqual(module["status"], "OK")
        for variable in ("cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high"):
            self.assertEqual(module["variable_status"][variable], "OK")
        self.assertEqual(module["required_unavailable_variables"], [])
        self.assertEqual(module["unavailable_variables"], [])

    def test_ec_ensemble_publishes_layer_distribution_and_probability(self):
        record = self.make_ecmwf_ensemble_record()
        distributions = pipeline.ensemble_daily_distributions(record["hourly"])
        first = distributions["variables"][0]
        self.assertEqual(first["members_valid"], 51)
        for variable in ("cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high"):
            stats = first["statistics"][variable]
            self.assertEqual(stats["available_members"], 51)
            self.assertLessEqual(stats["p10"], stats["median"])
            self.assertLessEqual(stats["median"], stats["p90"])
        probabilities = distributions["probabilities"][0]["probabilities"]
        for name in ("cloud_cover_low_gt_50pct", "cloud_cover_mid_gt_50pct", "cloud_cover_high_gt_50pct"):
            self.assertEqual(probabilities[name]["members_valid"], 51)
            self.assertIsNotNone(probabilities[name]["probability"])
        # Wind direction is not summarised with arithmetic percentiles.
        self.assertNotIn("wind_direction_10m", first["statistics"])
        self.assertTrue(first["statistics"]["wind_direction"]["circular_averaging"])
        window = pipeline._ensemble_window_view(record, date(2026, 10, 1), "AFTERNOON", date(2026, 10, 6))
        self.assertEqual(window["status"], "OK")
        self.assertEqual(window["members_valid"], 51)

    def test_gefs_null_layer_arrays_stay_optional_unavailable(self):
        # Reproduce the real Xinjiang GEFS behaviour: cloud_cover_low/mid/high
        # are returned as all-null arrays rather than missing keys.
        hourly = self.make_gefs_hourly()
        for variable in ("cloud_cover_low", "cloud_cover_mid", "cloud_cover_high"):
            for key in list(hourly):
                if key == variable or key.startswith(f"{variable}_member"):
                    hourly[key] = [None] * len(hourly["time"])
        valid, check = pipeline._gefs_member_check(hourly)
        self.assertTrue(valid)
        self.assertEqual(check["status"], "PASS")
        self.assertEqual(check["required_missing_variables"], [])
        self.assertEqual(
            set(check["optional_missing_variables"]),
            {"cloud_cover_low", "cloud_cover_mid", "cloud_cover_high"},
        )
        segment = pipeline._build_gefs_segment(
            {"hourly": hourly, "response": {"timezone": "Asia/Shanghai", "utc_offset_seconds": 28800}},
            "long_range",
            "ncep_gefs05",
            "GFS Ensemble 0.5°",
            "0.5° (~50 km)",
            date(2026, 10, 6),
        )
        self.assertEqual(segment["status"], "OK")
        self.assertEqual(segment["cloud_layer_status"]["cloud_cover"], "OK")
        for variable in ("cloud_cover_low", "cloud_cover_mid", "cloud_cover_high"):
            self.assertEqual(segment["cloud_layer_status"][variable], "OPTIONAL_UNAVAILABLE")
        self.assertEqual(segment["required_unavailable_variables"], [])
        self.assertEqual(
            set(segment["optional_unavailable_variables"]),
            {"cloud_cover_low", "cloud_cover_mid", "cloud_cover_high"},
        )
        statistics = segment["daily"][0]["statistics"]
        self.assertEqual(statistics["cloud_cover"]["available_members"], 31)
        # A null layer array yields no distribution: the total cloud cover is
        # never substituted into the missing layer.
        for variable in ("cloud_cover_low", "cloud_cover_mid", "cloud_cover_high"):
            self.assertEqual(statistics[variable]["available_members"], 0)
            self.assertIsNone(statistics[variable]["median"])
            probability = statistics["probabilities"][f"{variable}_gt_50pct"]
            self.assertIsNone(probability["probability"])
            self.assertEqual(probability["available_members"], 0)

    def test_missing_layer_is_never_derived_from_total_cloud(self):
        record = self.make_unified_deterministic_record()
        for key in ("cloud_cover_mid", "cloud_cover_high"):
            record["hourly"].pop(key)
        window = pipeline._deterministic_hourly_window(
            record, date(2026, 9, 30), "MORNING", date(2026, 10, 6)
        )
        self.assertEqual(window["total_cloud_pct"], 80.0)
        self.assertEqual(window["low_cloud_pct"], 60.0)
        self.assertIsNone(window["mid_cloud_pct"])
        self.assertIsNone(window["high_cloud_pct"])
        compact = pipeline._compact_deterministic_view(window)
        self.assertIsNone(compact["mid_cloud"])
        self.assertIsNone(compact["high_cloud"])

    def test_unavailable_variables_are_published_as_null_or_unavailable(self):
        unavailable = pipeline._compact_deterministic_view(
            {"status": "UNAVAILABLE", "reason": "DETERMINISTIC_MODULE_UNAVAILABLE"}
        )
        self.assertFalse(unavailable["available"])
        self.assertEqual(unavailable["status"], "UNAVAILABLE")
        self.assertEqual(unavailable["reason"], "DETERMINISTIC_MODULE_UNAVAILABLE")

        availability = pipeline.variable_availability(
            {"time": ["2026-10-01T00:00"], "cloud_cover": [10.0], "cloud_cover_mid": [None]},
            ["cloud_cover", "cloud_cover_mid", "cloud_cover_high"],
        )
        self.assertEqual(availability["cloud_cover"], "OK")
        self.assertEqual(availability["cloud_cover_mid"], "NULL_ARRAY")
        self.assertEqual(availability["cloud_cover_high"], "MISSING")
        status = pipeline.variable_status_classification(
            availability,
            required_variables=("cloud_cover",),
            optional_variables=("cloud_cover_mid", "cloud_cover_high"),
        )
        self.assertEqual(status["cloud_cover"], "OK")
        self.assertEqual(status["cloud_cover_mid"], "OPTIONAL_UNAVAILABLE")
        self.assertEqual(status["cloud_cover_high"], "OPTIONAL_UNAVAILABLE")
        self.assertEqual(
            pipeline.unavailable_variables_for(
                status,
                required_variables=("cloud_cover",),
                optional_variables=("cloud_cover_mid", "cloud_cover_high"),
            )["required_unavailable_variables"],
            [],
        )
        # An optional cloud capability never invalidates the whole segment.
        self.assertIn("OPTIONAL_UNAVAILABLE", pipeline.UNAVAILABLE_STATUSES)
        self.assertNotIn("OK", pipeline.UNAVAILABLE_STATUSES)

    def test_summary_compact_views_carry_every_unified_field(self):
        record = self.make_unified_deterministic_record()
        window = pipeline._deterministic_hourly_window(
            record, date(2026, 9, 30), "AFTERNOON", date(2026, 10, 6)
        )
        compact = pipeline._compact_deterministic_view(window)
        for field in (
            "cloud", "low_cloud", "mid_cloud", "high_cloud", "precip_mm", "rain_mm", "snow_cm",
            "temp_min_c", "temp_mean_c", "temp_max_c", "dew_point_c", "relative_humidity_pct",
            "wind_speed_kmh", "wind_direction_deg", "gust_kmh", "sunshine_or_shortwave",
        ):
            self.assertIn(field, compact, field)
        self.assertEqual(compact["precip_mm"], 0.4)
        self.assertEqual(compact["rain_mm"], 0.3)
        self.assertEqual(compact["dew_point_c"], 2.725)
        self.assertEqual(compact["relative_humidity_pct"], 70.0)
        self.assertEqual(compact["sunshine_or_shortwave"], {"variable": "sunshine_duration", "value": 7200.0})

        ensemble_record = self.make_ecmwf_ensemble_record()
        ensemble_view = pipeline._ensemble_window_view(
            ensemble_record, date(2026, 10, 1), "AFTERNOON", date(2026, 10, 6)
        )
        ensemble_compact = pipeline._compact_ensemble_view(ensemble_view)
        for field in (
            "cloud_median", "low_cloud_median", "mid_cloud_median", "high_cloud_median",
            "p_cloud_gt_70", "p_low_cloud_gt_50", "p_mid_cloud_gt_50", "p_high_cloud_gt_50",
            "p_precip", "p_snow", "wind_speed_median", "gust_p90", "temp_p10", "temp_median",
            "temp_p90", "dew_point_median", "relative_humidity_median", "layer_availability",
        ):
            self.assertIn(field, ensemble_compact, field)
        self.assertEqual(
            ensemble_compact["layer_availability"],
            {
                "cloud_cover": True,
                "cloud_cover_low": True,
                "cloud_cover_mid": True,
                "cloud_cover_high": True,
            },
        )

    def test_gust_and_sustained_wind_are_not_interchangeable(self):
        record = self.make_unified_deterministic_record()
        window = pipeline._deterministic_hourly_window(
            record, date(2026, 9, 30), "AFTERNOON", date(2026, 10, 6)
        )
        compact = pipeline._compact_deterministic_view(window)
        # Sustained mean wind and gust max come from different variables.
        self.assertEqual(compact["wind_speed_kmh"], 12.0)
        self.assertEqual(compact["gust_kmh"], 45.0)
        self.assertNotEqual(compact["wind_speed_kmh"], compact["gust_kmh"])

        member_values = [
            {
                "temperature_mean_c": 5.0,
                "temperature_min_c": 1.0,
                "temperature_max_c": 9.0,
                "wind_speed_kmh": 10.0,
                "wind_gust_kmh": 55.0 + index,
            }
            for index in range(31)
        ]
        summary = pipeline._gefs_distribution_summary(member_values, 31)
        self.assertEqual(summary["wind_speed_10m"]["median"], 10.0)
        self.assertGreaterEqual(summary["wind_gusts_10m"]["median"], 55.0)
        # The gust probability uses the gust series, not the sustained wind.
        self.assertEqual(summary["probabilities"]["gust_gt_50kmh"]["probability"], 1.0)
        self.assertNotEqual(summary["wind_speed_10m"]["median"], summary["wind_gusts_10m"]["median"])

    def test_wind_direction_uses_circular_averaging(self):
        self.assertAlmostEqual(pipeline.circular_mean_degrees([350, 10]), 0.0, places=3)
        self.assertNotAlmostEqual(pipeline.circular_mean_degrees([350, 10]), 180.0, places=1)
        self.assertAlmostEqual(pipeline.circular_mean_degrees([10, 20, 30]), 20.0, places=1)
        self.assertIsNone(pipeline.circular_mean_degrees([90, 270]))
        self.assertIsNone(pipeline.circular_mean_degrees([]))
        self.assertEqual(pipeline.circular_mean_degrees([360.0]), 0.0)
        self.assertAlmostEqual(pipeline.circular_resultant_length([10, 10]), 1.0, places=3)
        self.assertAlmostEqual(pipeline.circular_resultant_length([90, 270]), 0.0, places=3)
        statistics = pipeline.wind_direction_statistics([350, 10])
        self.assertEqual(statistics["mean_deg"], 0.0)
        self.assertTrue(statistics["circular_averaging"])
        self.assertEqual(statistics["sample_count"], 2)

        record = self.make_unified_deterministic_record()
        window = pipeline._deterministic_hourly_window(
            record, date(2026, 9, 30), "AFTERNOON", date(2026, 10, 6)
        )
        self.assertEqual(window["wind_direction_mean_deg"], 0.0)
        self.assertIsNotNone(window["wind_direction_resultant_length"])

    def test_viewing_conditions_separate_layers_and_do_not_punish_high_cloud(self):
        record = self.make_ecmwf_ensemble_record()
        ec_window = pipeline._ensemble_window_view(record, date(2026, 10, 1), "AFTERNOON", date(2026, 10, 6))
        gefs_like = copy.deepcopy(ec_window)
        signal = pipeline._viewing_signal(gefs_like, ec_window, {}, "HIGH")
        for field in (
            "total_cloud_signal", "low_cloud_signal", "mid_cloud_signal", "high_cloud_signal",
            "precip_signal", "snow_signal", "wind_signal", "visibility_related_signal",
            "model_agreement",
        ):
            self.assertIn(field, signal, field)
        self.assertIn("HIGH_CLOUD_IS_NOT_AUTOMATICALLY_BAD_WEATHER", signal["notes"])
        # High cloud alone must not force a bad-weather verdict.
        only_high = {
            "status": "OK",
            "statistics": {
                "cloud_cover": {"median": 5.0, "available_members": 51},
                "cloud_cover_low": {"median": 0.0, "available_members": 51},
                "cloud_cover_mid": {"median": 0.0, "available_members": 51},
                "cloud_cover_high": {"median": 90.0, "available_members": 51},
                "relative_humidity_2m": {"median": 40.0, "available_members": 51},
                "probabilities": {
                    "cloud_cover_high_gt_50pct": {"probability": 0.9, "available_members": 51},
                },
            },
        }
        high_only = pipeline._viewing_signal(only_high, only_high, {}, None)
        self.assertEqual(high_only["cloud_signal"], "CLEAR")
        self.assertEqual(high_only["high_cloud_signal"], "HIGH")
        self.assertIn("HIGH_CLOUD_ADDS_SKY_TEXTURE_AND_SUNRISE_SUNSET_POTENTIAL", high_only["notes"])
        # No source at all degrades to UNCERTAIN without crashing.
        none_signal = pipeline._viewing_signal({}, {}, {}, None)
        self.assertEqual(none_signal["total_cloud_signal"], "UNCERTAIN")
        self.assertEqual(none_signal["model_agreement"], "UNAVAILABLE")
        self.assertEqual(
            set(none_signal["layer_sources"].values()),
            {"UNAVAILABLE"},
        )

    def test_viewing_conditions_fall_back_per_layer_when_gefs_has_no_layers(self):
        # The real Xinjiang GEFS case: total cloud present, layered cloud null.
        gefs_without_layers = {
            "status": "OK",
            "statistics": {
                "cloud_cover": {"median": 55.0, "available_members": 31},
                "cloud_cover_low": {"median": None, "available_members": 0},
                "cloud_cover_mid": {"median": None, "available_members": 0},
                "cloud_cover_high": {"median": None, "available_members": 0},
                "relative_humidity_2m": {"median": None, "available_members": 0},
                "probabilities": {
                    "cloud_cover_gt_70pct": {"probability": 0.3, "available_members": 31},
                },
            },
        }
        ec_with_layers = {
            "status": "OK",
            "statistics": {
                "cloud_cover": {"median": 60.0, "available_members": 51},
                "cloud_cover_low": {"median": 70.0, "available_members": 51},
                "cloud_cover_mid": {"median": 45.0, "available_members": 51},
                "cloud_cover_high": {"median": 20.0, "available_members": 51},
                "relative_humidity_2m": {"median": 80.0, "available_members": 51},
                "probabilities": {
                    "cloud_cover_low_gt_50pct": {"probability": 0.9, "available_members": 51},
                    "cloud_cover_mid_gt_50pct": {"probability": 0.4, "available_members": 51},
                    "cloud_cover_high_gt_50pct": {"probability": 0.1, "available_members": 51},
                },
            },
        }
        signal = pipeline._viewing_signal(gefs_without_layers, ec_with_layers, {}, "HIGH")
        # Total cloud still comes from GEFS; the three layers come from ECMWF.
        self.assertEqual(signal["layer_sources"]["cloud_cover"], "gefs")
        self.assertEqual(signal["layer_sources"]["cloud_cover_low"], "ecmwf_ensemble")
        self.assertEqual(signal["layer_sources"]["cloud_cover_mid"], "ecmwf_ensemble")
        self.assertEqual(signal["layer_sources"]["cloud_cover_high"], "ecmwf_ensemble")
        self.assertEqual(signal["low_cloud_signal"], "HIGH")
        self.assertEqual(signal["mid_cloud_signal"], "MODERATE")
        self.assertEqual(signal["high_cloud_signal"], "LOW")
        self.assertEqual(signal["visibility_related_signal"], "HIGH")
        self.assertEqual(signal["model_agreement"], "HIGH")
        self.assertNotEqual(signal["low_cloud_signal"], "UNCERTAIN")

    def test_fog_inputs_publish_raw_indicators_without_probability(self):
        record = self.make_unified_deterministic_record(start=datetime(2026, 9, 29), days=4)
        fog = pipeline._fog_inputs(record, date(2026, 9, 30), date(2026, 10, 6))
        self.assertEqual(fog["status"], "OK")
        self.assertFalse(fog["probability_published"])
        for field in (
            "previous_12h_precip_mm", "previous_24h_precip_mm", "night_relative_humidity",
            "night_dew_point", "night_temp", "night_temp_dewpoint_spread",
            "pre_dawn_wind_speed", "pre_dawn_gust", "night_total_cloud",
            "night_low_cloud", "night_mid_cloud", "night_high_cloud",
            "moisture_signal", "radiative_cooling_signal", "wind_signal", "system_low_cloud_risk",
        ):
            self.assertIn(field, fog, field)
        serialized = json.dumps(fog, ensure_ascii=False)
        self.assertNotIn("probability_pct", serialized)
        self.assertNotIn("%", serialized)
        missing = pipeline._fog_inputs(None, date(2026, 9, 30), date(2026, 10, 6))
        self.assertEqual(missing["status"], "UNAVAILABLE")
        self.assertFalse(missing["probability_published"])

    def test_model_consistency_never_averages_models(self):
        hres = {
            "status": "OK",
            "total_cloud_pct": 80.0,
            "low_cloud_pct": 60.0,
            "mid_cloud_pct": 30.0,
            "high_cloud_pct": 10.0,
            "temperature_mean_c": 6.0,
            "precipitation_mm": 0.0,
            "snowfall_cm": 0.0,
            "wind_speed_mean_kmh": 12.0,
            "gust_max_kmh": 45.0,
        }
        gfs = dict(hres)
        ec_ens = {
            "status": "OK",
            "statistics": {
                "cloud_cover": {"p10": 40.0, "p25": 50.0, "p75": 70.0, "p90": 80.0, "median": 75.0, "available_members": 51},
                "cloud_cover_low": {"p10": 40.0, "p25": 50.0, "p75": 70.0, "p90": 80.0, "median": 55.0, "available_members": 51},
                # The layer is absent, exactly like a null-array GEFS response.
                "probabilities": {},
            },
        }
        gefs = copy.deepcopy(ec_ens)
        consistency = pipeline._model_consistency(hres, gfs, ec_ens, gefs)
        self.assertEqual(consistency["averaging_policy"], "NO_CROSS_MODEL_AVERAGING")
        self.assertEqual(
            set(consistency),
            {
                "ec_hres_vs_ec_ensemble",
                "gfs_deterministic_vs_gefs",
                "ec_hres_vs_gfs_deterministic",
                "ec_ensemble_vs_gefs",
                "averaging_policy",
            },
        )
        pair = consistency["ec_hres_vs_ec_ensemble"]
        self.assertEqual(pair["cloud_cover"], "INSIDE_P10_P90")
        self.assertEqual(pair["low_cloud"], "INSIDE_IQR")
        # A layer the ensemble cannot supply stays UNAVAILABLE instead of being
        # compared against the total cloud cover.
        self.assertEqual(pair["mid_cloud"], "UNAVAILABLE")
        self.assertEqual(pair["high_cloud"], "UNAVAILABLE")
        deterministic_pair = consistency["ec_hres_vs_gfs_deterministic"]
        self.assertEqual(deterministic_pair["variables"]["temperature_mean_c"]["comparison"], "HIGH")
        self.assertEqual(deterministic_pair["variables"]["gust_max_kmh"]["comparison"], "HIGH")
        # Two ensembles with identical medians agree, but they are compared --
        # never merged into one averaged value.
        self.assertEqual(consistency["ec_ensemble_vs_gefs"]["agreement"], "HIGH")
        self.assertEqual(
            consistency["ec_ensemble_vs_gefs"]["method"],
            "independent comparison, no cross-model averaging",
        )

    def test_solar_probe_failure_does_not_mark_a_delivered_variable_missing(self):
        # GEFS 0.5°: the unified request delivers sunshine_duration, while the
        # standalone shortwave_radiation probe fails.
        hourly = {"time": ["2026-10-01T00:00", "2026-10-01T01:00"], "sunshine_duration": [10.0, 20.0]}
        self.assertEqual(
            pipeline._solar_variables_still_missing({"hourly": hourly}),
            ["shortwave_radiation"],
        )
        self.assertTrue(pipeline._gefs_solar_available({"hourly": hourly}))
        # Neither alternative delivered: both are genuinely missing.
        self.assertEqual(
            pipeline._solar_variables_still_missing({"hourly": {"time": ["2026-10-01T00:00"]}}),
            ["shortwave_radiation", "sunshine_duration"],
        )
        # The probe's own variable delivered, the other not.
        self.assertEqual(
            pipeline._solar_variables_still_missing(
                {"hourly": {"time": ["2026-10-01T00:00"], "shortwave_radiation": [1.0]}}
            ),
            ["sunshine_duration"],
        )
        # An all-null array is not a delivered value.
        self.assertEqual(
            pipeline._solar_variables_still_missing(
                {"hourly": {"time": ["2026-10-01T00:00"], "sunshine_duration": [None]}}
            ),
            ["shortwave_radiation", "sunshine_duration"],
        )
        self.assertFalse(pipeline._gefs_solar_available({"hourly": {"time": [], "sunshine_duration": []}}))

        # A stale probe verdict recorded in gefs_missing_variables must not
        # downgrade a variable the response actually delivered.
        gefs_hourly = self.make_gefs_hourly()
        for variable in ("cloud_cover_low", "cloud_cover_mid", "cloud_cover_high"):
            for key in list(gefs_hourly):
                if key == variable or key.startswith(f"{variable}_member"):
                    gefs_hourly[key] = [None] * len(gefs_hourly["time"])
        segment = pipeline._build_gefs_segment(
            {
                "hourly": gefs_hourly,
                "solar_variable": "sunshine_duration",
                "gefs_missing_variables": ["shortwave_radiation", "sunshine_duration"],
                "response": {"timezone": "Asia/Shanghai", "utc_offset_seconds": 28800},
            },
            "long_range",
            "ncep_gefs05",
            "GFS Ensemble 0.5°",
            "0.5° (~50 km)",
            date(2026, 10, 6),
        )
        self.assertEqual(segment["status"], "OK")
        self.assertEqual(segment["variable_status"]["sunshine_duration"], "PARTIAL")

        corrected = pipeline._build_gefs_segment(
            {
                "hourly": gefs_hourly,
                "solar_variable": "sunshine_duration",
                "gefs_missing_variables": ["shortwave_radiation"],
                "response": {"timezone": "Asia/Shanghai", "utc_offset_seconds": 28800},
            },
            "long_range",
            "ncep_gefs05",
            "GFS Ensemble 0.5°",
            "0.5° (~50 km)",
            date(2026, 10, 6),
        )
        self.assertEqual(corrected["variable_status"]["sunshine_duration"], "OK")
        self.assertIn("shortwave_radiation", corrected["optional_unavailable_variables"])
        self.assertNotIn("sunshine_duration", corrected["optional_unavailable_variables"])
        self.assertEqual(
            corrected["optional_unavailable_variables"],
            sorted(set(corrected["optional_unavailable_variables"])),
        )

    def test_schema_version_and_new_contracts_validate(self):
        root = Path(__file__).resolve().parents[1]
        # Schemas that describe a pipeline output artifact must track SCHEMA_VERSION.
        artifact_schemas = {
            "gefs.schema.json",
            "grid_registry.schema.json",
            "history_cache.schema.json",
            "history_forward.schema.json",
            "long_range.schema.json",
            "module.schema.json",
            "phenology_weather_summary.schema.json",
            "status.schema.json",
            "summary.schema.json",
            "weather_events.schema.json",
            "weather_events_cache.schema.json",
        }
        for path in sorted((root / "schemas").glob("*.schema.json")):
            schema = json.loads(path.read_text(encoding="utf-8"))
            # check_schema raises SchemaError when the document itself is invalid.
            Draft202012Validator.check_schema(schema)
            const = (schema.get("properties", {}).get("schema_version") or {}).get("const")
            if const is None:
                continue
            if path.name in artifact_schemas:
                self.assertEqual(const, pipeline.SCHEMA_VERSION, path.name)
            else:
                # A schema that keeps its own version stays compatible instead of
                # having to track the artifact SCHEMA_VERSION.
                self.assertIn(const, pipeline.COMPATIBLE_SCHEMA_VERSIONS, path.name)

        summary_schema = json.loads((root / "schemas" / "summary.schema.json").read_text(encoding="utf-8"))
        defs = summary_schema["$defs"]
        root_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$ref": "#/$defs/targetWindowLocation",
            "$defs": defs,
        }
        record = self.make_unified_deterministic_record()
        target = date(2026, 9, 30)
        cutoff = date(2026, 10, 6)
        deterministic = pipeline._compact_deterministic_view(
            pipeline._deterministic_hourly_window(record, target, "MORNING", cutoff)
        )
        ensemble = pipeline._compact_ensemble_view(
            pipeline._ensemble_window_view(
                self.make_ecmwf_ensemble_record(), date(2026, 10, 1), "MORNING", cutoff
            )
        )
        if ensemble["available"] is False:
            ensemble = {
                "available": True,
                "status": "OK",
                "cloud_median": 40.0,
                "low_cloud_median": None,
                "mid_cloud_median": None,
                "high_cloud_median": None,
                "layer_availability": {
                    "cloud_cover": True,
                    "cloud_cover_low": False,
                    "cloud_cover_mid": False,
                    "cloud_cover_high": False,
                },
            }
        window = {
            "ec_det": deterministic,
            "gfs_det": deterministic,
            "ec_ens": ensemble,
            "gefs": ensemble,
            "viewing_conditions": pipeline._viewing_signal({}, {}, {}, None),
        }
        payload = {
            "location_id": "siguniang",
            "location_name": "四姑娘山",
            "usable_for_main_chain": True,
            "forecast_granularity": "hourly_window_supported",
            "daily": {"ec_det": deterministic, "gfs_det": deterministic},
            "morning": window,
            "afternoon": window,
            "night": window,
            "fog_inputs": pipeline._fog_inputs(record, target, cutoff),
            "event_phase": {},
        }
        self.assertEqual(list(Draft202012Validator(root_schema).iter_errors(payload)), [])

        status_schema = json.loads((root / "schemas" / "status.schema.json").read_text(encoding="utf-8"))
        variable_status_validator = Draft202012Validator(status_schema["$defs"]["variableStatus"])
        self.assertEqual(
            list(variable_status_validator.iter_errors({
                "temperature_2m": "OK",
                "cloud_cover_mid": "OPTIONAL_UNAVAILABLE",
            })),
            [],
        )

    def make_single_run_record(self, point, init_time, horizon_hours):
        start_local = init_time + timedelta(hours=8)
        times = [
            (start_local + timedelta(hours=index)).strftime("%Y-%m-%dT%H:%M")
            for index in range(horizon_hours)
        ]
        hourly = {"time": times}
        for variable in pipeline.SINGLE_RUN_VARIABLES:
            hourly[variable] = [1.0] * len(times)
        return {
            "point_id": point["id"],
            "point": point,
            "status": "PASS",
            "source": "Open-Meteo",
            "endpoint": pipeline.OPEN_METEO_ENDPOINTS["single_runs"],
            "model": "ECMWF IFS HRES 9 km",
            "solar_variable": "sunshine_duration",
            "hourly": hourly,
            "request": {"parameters": {}},
            "response": {
                "grid_coordinate": {"latitude": point["latitude"], "longitude": point["longitude"]},
                "returned_elevation": 1000,
                "timezone": "Asia/Shanghai",
                "utc_offset_seconds": 28800,
            },
            "qa": {
                "final_status": "PASS",
                "grid_distance_km": 0,
                "grid_distance_limit_km": pipeline.HRES_GRID_QA_LIMIT_KM,
            },
        }

    def test_closed_history_window_folds_into_unavailable_in_lightweight_views(self):
        definition = {
            "window": "d16_to_11_01",
            "status": pipeline.HISTORY_FORWARD_WINDOW_NOT_APPLICABLE,
            "reason": "WINDOW_AFTER_CUTOFF",
            "start_date": None,
            "end_date": None,
        }
        summary = pipeline.lightweight_window_summary([], definition)
        # The history-forward contract keeps the structural state...
        self.assertEqual(summary["status"], pipeline.HISTORY_FORWARD_WINDOW_NOT_APPLICABLE)
        # ... while the lightweight views only know OK/PARTIAL/INVALID/UNAVAILABLE.
        flattened = pipeline.flattened_lightweight_window(summary)
        self.assertEqual(flattened["status"], "UNAVAILABLE")
        self.assertEqual(flattened["reason"], "WINDOW_AFTER_CUTOFF")
        with (ROOT / "schemas" / "phenology_weather_summary.schema.json").open(encoding="utf-8") as handle:
            schema = json.load(handle)
        self.assertEqual(
            list(Draft202012Validator(schema["$defs"]["light_window"]).iter_errors(flattened)), []
        )

    def test_history_forward_empty_window_is_not_applicable_and_does_not_fail_the_module(self):
        config = pipeline.load_config()
        requests = []

        def fake_fetch(_client, **kwargs):
            requests.append(kwargs["params"])
            return self.make_history_forward_record(kwargs["point"], kwargs["params"])

        # 2026-10-17 is the first anchor date whose d16_to_11_01 window starts
        # past the 11-01 cutoff: the window becomes structurally empty.
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(pipeline, "HISTORY_CACHE_DIR", Path(tmp)):
                with patch.object(pipeline, "fetch_point", side_effect=fake_fetch):
                    result = pipeline.run_history_forward(
                        config,
                        object(),
                        "2026-10-17T00:00:00Z",
                        "2026-10-16",
                        date(2026, 10, 17),
                    )

        definitions = {item["window"]: item for item in result["window_definitions"]}
        self.assertEqual(
            definitions["d16_to_11_01"]["status"],
            pipeline.HISTORY_FORWARD_WINDOW_NOT_APPLICABLE,
        )
        self.assertEqual(
            pipeline.history_forward_applicable_window_keys(result["window_definitions"]),
            ["d0_7", "d8_15"],
        )
        self.assertEqual(result["status"], "OK")
        for point in result["points"].values():
            self.assertEqual(point["status"], "OK")
            self.assertTrue(point["cross_year_comparison_usable"])
            for year in ("2023", "2024", "2025"):
                self.assertEqual(
                    point["years"][year]["d16_to_11_01"]["status"],
                    pipeline.HISTORY_FORWARD_WINDOW_NOT_APPLICABLE,
                )
                self.assertEqual(
                    point["years"][year]["d16_to_11_01"]["reason"],
                    "WINDOW_AFTER_CUTOFF",
                )
        for region_id in ("lixiaolu", "jiuzhaigou", "siguniang"):
            self.assertEqual(result["regions"][region_id]["status"], "OK")
        expected = len(pipeline.history_forward_point_ids(config)) * 3
        self.assertEqual(result["expected_fetches"], expected)
        self.assertEqual(result["successful_fetches"], expected)
        self.assertEqual(len(requests), expected)
        self.assert_history_forward_schema_valid(result)

    def test_history_forward_closed_season_is_skipped_without_fetching(self):
        config = pipeline.load_config()
        calls = []

        def fake_fetch(_client, **kwargs):
            calls.append(kwargs["params"])
            return self.make_history_forward_record(kwargs["point"], kwargs["params"])

        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(pipeline, "HISTORY_CACHE_DIR", Path(tmp)):
                with patch.object(pipeline, "fetch_point", side_effect=fake_fetch):
                    result = pipeline.run_history_forward(
                        config,
                        object(),
                        "2026-11-02T00:00:00Z",
                        "2026-11-02",
                        date(2026, 11, 2),
                    )

        self.assertEqual(result["status"], "SKIPPED")
        self.assertEqual(calls, [])
        self.assertEqual(result["expected_fetches"], 0)
        self.assertEqual(result["successful_fetches"], 0)
        self.assertEqual(result["failed_fetches"], 0)
        self.assertEqual(pipeline.history_forward_applicable_window_keys(result["window_definitions"]), [])
        for item in result["window_definitions"]:
            self.assertEqual(item["status"], pipeline.HISTORY_FORWARD_WINDOW_NOT_APPLICABLE)
        for point in result["points"].values():
            self.assertEqual(point["status"], pipeline.HISTORY_FORWARD_WINDOW_NOT_APPLICABLE)
            self.assertFalse(point["usable_for_main_chain"])
            self.assertEqual(point["reason"], pipeline.HISTORY_FORWARD_WINDOW_CLOSED_REASON)
        for year in ("2023", "2024", "2025"):
            self.assertEqual(
                result["regions"]["jiuzhaigou"]["years"][year]["d16_to_11_01"]["status"],
                pipeline.HISTORY_FORWARD_WINDOW_NOT_APPLICABLE,
            )
        self.assert_history_forward_schema_valid(result)

    def assert_history_forward_schema_valid(self, result):
        """The published NOT_APPLICABLE / SKIPPED states must stay schema-valid."""
        with (ROOT / "schemas" / "history_forward.schema.json").open(encoding="utf-8") as handle:
            schema = json.load(handle)
        errors = sorted(Draft202012Validator(schema).iter_errors(result), key=lambda e: list(e.path))
        self.assertEqual([(list(e.path), e.message) for e in errors], [])

    def test_single_runs_only_requires_the_full_horizon_cycles(self):
        config = pipeline.load_config()
        horizons = []

        def fake_fetch(_client, **kwargs):
            init_time = datetime.strptime(kwargs["params"]["run"], "%Y-%m-%dT%H:%M")
            short = init_time.hour in pipeline.SINGLE_RUN_SHORT_CYCLE_HOURS
            horizon = 144 if short else 240
            horizons.append((init_time.hour, horizon))
            return self.make_single_run_record(kwargs["point"], init_time, horizon)

        # Anchored inside the 10-day single-run horizon of the 2026-10-24
        # Siguniang / Lixiaolu visit date.
        now_utc = datetime(2026, 10, 18, 1, 46, tzinfo=timezone.utc)
        with patch.object(pipeline, "fetch_point", side_effect=fake_fetch):
            result = pipeline.run_single_runs(
                config,
                object(),
                "2026-10-18T01:46:00Z",
                "2026-10-17",
                now_utc,
            )

        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["required_runs_requested"], 4)
        self.assertEqual(result["short_runs_requested"], 4)
        lixiaolu = result["regions"]["lixiaolu"]
        self.assertEqual(lixiaolu["target_time"], "2026-10-24T05:00+08:00")
        self.assertEqual(lixiaolu["status"], "OK")
        self.assertEqual(lixiaolu["run_count_requested"], 8)
        self.assertEqual(lixiaolu["required_run_count_requested"], 4)
        self.assertEqual(lixiaolu["required_run_count_available"], 4)
        self.assertEqual(lixiaolu["short_run_count_requested"], 4)
        self.assertEqual(lixiaolu["short_run_count_available"], 0)
        self.assertFalse(lixiaolu["target_reachable_by_short_runs"])
        # The old 8/8 rule failed this region even though every full-horizon
        # cycle was present and usable.
        self.assertEqual(lixiaolu["run_count_available"], 4)
        self.assertEqual(len(lixiaolu["runs"]), 8)
        for entry in lixiaolu["runs"]:
            if entry["cycle_class"] == "SHORT":
                self.assertEqual(entry["status"], "INVALID")
                self.assertEqual(entry["target"]["reason"], "TARGET_TIME_NOT_IN_RUN")
                self.assertEqual(entry["forecast_horizon_hours"], 144)
            else:
                self.assertEqual(entry["status"], "PASS")
                self.assertEqual(entry["forecast_horizon_hours"], 240)

    def test_single_runs_is_partial_when_a_full_horizon_cycle_is_unavailable(self):
        config = pipeline.load_config()

        def fake_fetch(_client, **kwargs):
            init_time = datetime.strptime(kwargs["params"]["run"], "%Y-%m-%dT%H:%M")
            if init_time.hour == 0:
                return {"status": "INVALID", "point_id": kwargs["point"]["id"], "hourly": {}, "qa": {"final_status": "INVALID"}}
            short = init_time.hour in pipeline.SINGLE_RUN_SHORT_CYCLE_HOURS
            return self.make_single_run_record(kwargs["point"], init_time, 144 if short else 240)

        now_utc = datetime(2026, 9, 24, 1, 46, tzinfo=timezone.utc)
        with patch.object(pipeline, "fetch_point", side_effect=fake_fetch):
            result = pipeline.run_single_runs(
                config,
                object(),
                "2026-09-24T01:46:00Z",
                "2026-09-23",
                now_utc,
            )

        self.assertEqual(result["status"], "PARTIAL")
        lixiaolu_region = result["regions"]["lixiaolu"]
        self.assertEqual(lixiaolu_region["status"], "PARTIAL")
        self.assertEqual(lixiaolu_region["status_reason"], "SINGLE_RUN_LONG_CYCLE_PARTIALLY_DISTRIBUTED")
        self.assertEqual(lixiaolu_region["required_run_count_available"], pipeline.SINGLE_RUN_MIN_REQUIRED_RUNS)

    def test_long_range_horizon_requires_the_deliverable_block_range(self):
        hourly = self.make_long_range_hourly()
        _origin, daily_by_lead = pipeline.long_range_daily_member_values(hourly)

        full = pipeline.long_range_horizon_check(daily_by_lead)
        self.assertEqual(full["status"], "PASS")
        self.assertEqual(full["expected_lead_day_range"], [0, pipeline.LONG_RANGE_LEAD_END])
        self.assertEqual(full["required_lead_day_range"], [0, pipeline.LONG_RANGE_REQUIRED_LEAD_END])
        self.assertEqual(full["required_forecast_days"], 34)
        self.assertEqual(full["missing_required_lead_days"], [])

        deliverable = {
            lead: values
            for lead, values in daily_by_lead.items()
            if lead <= pipeline.LONG_RANGE_REQUIRED_LEAD_END
        }
        self.assertEqual(pipeline.long_range_horizon_check(deliverable)["status"], "PASS")

        short_but_contiguous = {
            lead: values for lead, values in daily_by_lead.items() if lead <= 30
        }
        partial = pipeline.long_range_horizon_check(short_but_contiguous)
        self.assertEqual(partial["status"], "PARTIAL")
        self.assertEqual(partial["missing_required_lead_days"], [31, 32, 33])

        too_short = {
            lead: values for lead, values in daily_by_lead.items() if lead <= 10
        }
        self.assertEqual(pipeline.long_range_horizon_check(too_short)["status"], "FAIL")

        holed = {
            lead: values for lead, values in daily_by_lead.items() if lead != 30
        }
        self.assertEqual(pipeline.long_range_horizon_check(holed)["status"], "FAIL")

    def test_long_range_edge_truncation_only_counts_inside_the_required_range(self):
        daily_by_lead = {
            0: {"temperature_2m": {"date": "2026-09-24"}},
            33: {"temperature_2m": {"date": "2026-10-27"}},
            35: {"temperature_2m": {"date": "2026-10-29"}},
        }

        def member_check(first: str, last: str) -> dict:
            variables = ("precipitation", "snowfall")
            return {
                "edge_truncated_variables": list(variables),
                "variable_availability": {
                    variable: {
                        "first_timestamp": first,
                        "last_timestamp": last,
                        "edge_truncated": True,
                    }
                    for variable in variables
                },
            }

        # Precipitation/snowfall stopping inside the trailing D34_D35 block is a
        # property of ncep_gefs05, not a data failure.
        edge_only = member_check("2026-09-24T00:00", "2026-10-28T12:00")
        self.assertEqual(
            pipeline.long_range_variable_horizon_offenders(edge_only, daily_by_lead), []
        )

        # A variable that stops before the required last block must still downgrade.
        inside_required = member_check("2026-09-24T00:00", "2026-10-14T12:00")
        self.assertEqual(
            pipeline.long_range_variable_horizon_offenders(inside_required, daily_by_lead),
            ["precipitation", "snowfall"],
        )

        # A missing leading edge is never an acceptable edge truncation.
        lead_trimmed = member_check("2026-09-25T00:00", "2026-10-28T12:00")
        self.assertEqual(
            pipeline.long_range_variable_horizon_offenders(lead_trimmed, daily_by_lead),
            ["precipitation", "snowfall"],
        )

        self.assertEqual(pipeline.long_range_variable_horizon_offenders({}, daily_by_lead), [])


if __name__ == "__main__":
    unittest.main()
