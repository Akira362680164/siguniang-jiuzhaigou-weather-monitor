import copy
import json
import sys
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import pipeline


class PipelineUnitTests(unittest.TestCase):
    def test_config_has_both_target_windows_and_fixed_points(self):
        config = pipeline.load_config()
        self.assertEqual(set(config["target_groups"]), {"siguniang_2026_10_24_25", "jiuzhaigou_2026_10_31_11_01"})
        self.assertEqual(len(pipeline.active_points(config)), 7)
        self.assertIn("LXL_HIGH_PASS", config["target_groups"]["siguniang_2026_10_24_25"]["point_ids"])
        self.assertIn("JZG_LONGHAI", config["target_groups"]["jiuzhaigou_2026_10_31_11_01"]["point_ids"])

    def test_haversine_distance(self):
        self.assertEqual(pipeline.haversine_km(0, 0, 0, 0), 0)
        self.assertAlmostEqual(pipeline.haversine_km(31.1035, 102.9239, 31.1035, 102.9239), 0, places=8)

    def test_daily_metrics_tracks_temperature_rain_and_cloud(self):
        times = []
        for hour in range(24):
            times.append(f"2026-09-22T{hour:02d}:00")
        hourly = {
            "time": times,
            "temperature_2m": [hour - 5 for hour in range(24)],
            "precipitation": [0.25 if hour in (3, 4) else 0 for hour in range(24)],
            "snowfall": [0.5 if hour == 4 else 0 for hour in range(24)],
            "cloud_cover": [80 if hour >= 12 else 20 for hour in range(24)],
            "cloud_cover_low": [60 if hour >= 12 else 10 for hour in range(24)],
            "wind_speed_10m": [10] * 24,
            "wind_gusts_10m": [20] * 24,
            "relative_humidity_2m": [70] * 24,
        }
        day = pipeline.daily_metrics(hourly, "hres")[0]
        self.assertEqual(day["date"], "2026-09-22")
        self.assertEqual(day["precipitation_mm"], 0.5)
        self.assertEqual(day["snowfall_cm"], 0.5)
        self.assertEqual(day["cloud_cover_mean_pct"], 50.0)
        self.assertEqual(day["temperature_min_c"], -5.0)

    def test_accumulation_deduplicates_three_hour_plateaus(self):
        times = [f"2026-09-22T{hour:02d}:00" for hour in range(6)]
        hourly = {"time": times}
        for suffix in ("", "_member01"):
            hourly[f"temperature_2m{suffix}"] = [1] * 6
            hourly[f"precipitation{suffix}"] = [0.8, 0.8, 0.8, 0.0, 0.0, 0.0]
            hourly[f"snowfall{suffix}"] = [0] * 6
        day = pipeline.member_daily_distributions(hourly, 2, native_step_hours=3)[0]
        self.assertEqual(day["precipitation_mm"]["mean"], 0.8)
        self.assertEqual(day["probabilities"]["precipitation_gt_0_5mm"], 1.0)
        self.assertEqual(day["probabilities"]["precipitation_gt_5mm"], 0.0)

    def test_coarse_model_specs_declare_native_three_hour_resolution(self):
        self.assertEqual(pipeline.MODEL_SPECS["ensemble"]["temporal_resolution"], "hourly_3")
        self.assertEqual(pipeline.GEFS_SPECS["long_range"]["temporal_resolution"], "hourly_3")

    def test_validate_payload_keeps_optional_cloud_warning_as_partial(self):
        point = {"id": "P", "latitude": 31.1, "longitude": 102.9}
        payload = {
            "latitude": 31.1,
            "longitude": 102.9,
            "elevation": 3000,
            "timezone": "Asia/Shanghai",
            "utc_offset_seconds": 28800,
            "hourly": {
                "time": ["2026-09-22T00:00"],
                "temperature_2m": [1],
                "precipitation": [0],
            },
        }
        qa = pipeline.validate_payload(
            payload,
            point,
            ["temperature_2m", "precipitation", "cloud_cover"],
            ["temperature_2m"],
            1,
        )
        self.assertTrue(qa["valid"])
        self.assertEqual(qa["final_status"], "PARTIAL")
        self.assertIn("cloud_cover", qa["optional_missing_variables"])

    def test_member_daily_distribution_and_probability(self):
        times = [f"2026-09-22T{hour:02d}:00" for hour in range(4)]
        hourly = {"time": times}
        for suffix, offset in (("", 0), ("_member01", 2), ("_member02", -2)):
            hourly[f"temperature_2m{suffix}"] = [1 + offset] * 4
            hourly[f"precipitation{suffix}"] = [1] * 4
            hourly[f"snowfall{suffix}"] = [0.2] * 4
            hourly[f"cloud_cover{suffix}"] = [80] * 4
            hourly[f"cloud_cover_low{suffix}"] = [60] * 4
            hourly[f"wind_gusts_10m{suffix}"] = [45] * 4
        day = pipeline.member_daily_distributions(hourly, 3)[0]
        self.assertEqual(day["members_valid"], 3)
        self.assertEqual(day["temperature_mean_c"]["median"], 1.0)
        self.assertEqual(day["probabilities"]["precipitation_gt_0_5mm"], 1.0)
        self.assertEqual(day["probabilities"]["cloud_cover_gt_70pct"], 1.0)

    def test_target_summary_does_not_fill_unavailable_dates(self):
        config = pipeline.load_config()
        generated = "2026-09-22T00:00:00Z"
        modules = {}
        for name in ("hres", "gfs", "ensemble"):
            modules[name] = {"points": {}}
        modules["gefs"] = {"points": {}}
        summary = pipeline.build_target_summary(config, generated, "2026-09-22", modules)
        entry = summary["target_groups"]["jiuzhaigou_2026_10_31_11_01"]["by_date"]["2026-10-31"]["points"]["JZG_NORILANG"]
        self.assertEqual(entry["coverage_status"], "NOT_YET_AVAILABLE")
        self.assertFalse(any(entry["sources_available"].values()))
        self.assertIsNone(entry["metrics"]["consensus"]["values"]["temperature_mean_c"])

    def test_target_summary_uses_gefs_when_only_long_range_exists(self):
        config = pipeline.load_config()
        day = {"date": "2026-10-24", "temperature_mean_c": {"mean": 4.2}, "precipitation_mm": {"mean": 0.8}, "cloud_cover_mean_pct": {"mean": 72}}
        modules = {
            "hres": {"points": {}},
            "gfs": {"points": {}},
            "ensemble": {"points": {}},
            "gefs": {"points": {"SQG_SHUANGQIAO": {"long_range": {"record": {"ensemble_daily": [day]}}}}},
        }
        summary = pipeline.build_target_summary(config, "2026-09-22T00:00:00Z", "2026-09-22", modules)
        entry = summary["target_groups"]["siguniang_2026_10_24_25"]["by_date"]["2026-10-24"]["points"]["SQG_SHUANGQIAO"]
        self.assertEqual(entry["coverage_status"], "GEFS_LONG_RANGE_AVAILABLE")
        self.assertEqual(entry["confidence_class"], "long_range_trend")
        self.assertEqual(entry["metrics"]["consensus"]["values"]["temperature_mean_c"], 4.2)

    def test_status_and_target_schema(self):
        config = pipeline.load_config()
        target = pipeline.build_target_summary(
            config,
            "2026-09-22T00:00:00Z",
            "2026-09-22",
            {"hres": {"points": {}}, "gfs": {"points": {}}, "ensemble": {"points": {}}, "gefs": {"points": {}}},
        )
        status = pipeline.build_status(config, "2026-09-22T00:00:00Z", "2026-09-22", {"hres": {"status": "OK"}}, target)
        for schema_name, instance in (("target_summary.schema.json", target), ("status.schema.json", status)):
            schema = json.loads((ROOT / "schemas" / schema_name).read_text(encoding="utf-8"))
            errors = list(Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER).iter_errors(instance))
            self.assertEqual(errors, [], msg=f"{schema_name}: {errors}")


if __name__ == "__main__":
    unittest.main()
