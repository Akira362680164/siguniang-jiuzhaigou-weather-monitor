# JSON Schema

`status.json` and `summary.json` are the stable machine-readable contracts. The
module artifacts use the common envelope in `module.schema.json`; module-specific
fields are intentionally additive so a new QA detail can be added without changing
the meaning of existing fields.

Schema version `1.4.0` is an additive update over `1.3.0`; all v1.0.0–v1.3.0 fields
keep their meaning. `COMPATIBLE_SCHEMA_VERSIONS` accepts `1.0.0`, `1.1.0`, `1.2.0`,
`1.3.0` and `1.4.0` so existing archives stay readable without backfill.

`1.3.0` added the independent `gefs` module and the compact
`summary.json.target_window_brief` view. GEFS is kept separate from ECMWF Ensemble
and is used to test GFS deterministic support, event existence and phase spread; the
two ensembles are never averaged. The `weather_events` module and
`weather_events_cache` schema describe derived weather events sourced only from the
existing Historical cache and the current HRES forecast. A breaking change must
increment the major version and update the schemas, tests, README, and Raw entry
points together. An omitted or `null` numeric value means that the value was
unavailable or failed QA; it is never an inferred replacement value.

## Trip-facing artifacts specific to this monitor

Two artifacts are published in addition to the shared module set. Both document the
configured `target_groups`: the Siguniang weekend `2026-10-24`–`2026-10-25` and the
Jiuzhaigou weekend `2026-10-31`–`2026-11-01`.

- `target_summary.json` — per target group / target date / point coverage. Each
  point states which model family actually covers that target date
  (`coverage_status`), how many days ahead it is (`lead_days`), the supporting
  distributions, and a deterministic + ensemble + GEFS consensus. Member statistics
  are normalised across the two upstream distribution layouts before they are
  published here, so a reader sees one vocabulary regardless of which module
  produced the point record.
- `historical_comparison.json` — the three-year archive comparison. For each target
  group it queries the Open-Meteo archive endpoint over the target window
  ±`window_days_each_side` for `historical_comparison.years`, and publishes per-day
  rows plus a `window_summary` with a day-part split (`night_00_06` /
  `morning_06_12` / `afternoon_12_18` / `evening_18_24`).

`status.schema.json` documents both through `target_coverage` and `target_dates`, and
reports `target_summary` / `historical_comparison` in the same `modules` map as the
forecast modules. They are supplementary, but a failure there still downgrades
`pipeline_status`, because the published picture would otherwise be silently
incomplete.

## Unified weather variable system (1.4.0)

Every forecast model — ECMWF deterministic (HRES), ECMWF ensemble, GFS deterministic
and the independent GEFS ensemble — now requests and reports the same vocabulary:

- temperature: `temperature_2m`, `dew_point_2m`, `relative_humidity_2m`;
- precipitation: `precipitation`, `rain`, `snowfall`;
- cloud: `cloud_cover`, `cloud_cover_low`, `cloud_cover_mid`, `cloud_cover_high`;
- wind: `wind_speed_10m` (sustained), `wind_direction_10m` (circular), `wind_gusts_10m` (gust);
- radiation: `sunshine_duration`, falling back to `shortwave_radiation`.

Rules that the schemas and the pipeline enforce:

1. **Open-Meteo is the only numeric source.** No other weather site, app or model
   may fill a value.
2. **The four model families stay independent.** Values are compared, never averaged
   across models, and never averaged between ECMWF ensemble and GEFS.
3. **Layered cloud always comes from the API.** `cloud_cover_mid`/`cloud_cover_high`
   are never derived by subtracting low cloud from the total; the layers overlap and
   are not additive. When a source returns null arrays for a layer, that layer stays
   `OPTIONAL_UNAVAILABLE`.
4. **Required / optional boundary.** A required (core) variable that is unavailable
   makes the module `PARTIAL`/`FAILED`. An optional variable that is unavailable is
   recorded as `OPTIONAL_UNAVAILABLE` and must not fail the module.
5. **Gust ≠ sustained wind.** `wind_gusts_10m` and `wind_speed_10m` are separate
   fields in every artifact.
6. **Wind direction is circular.** It is stored hourly and reduced only with a vector
   mean; ensemble distributions never contain an arithmetic percentile for
   `wind_direction_10m`.
7. **Unavailable never becomes a number.** A missing value is `null` (or
   `UNAVAILABLE` in a status field), never `0`.

### `variable_status` vocabulary

Published per model in `status.json.variable_status` and per variable inside the
module artifacts:

| Value | Meaning |
|---|---|
| `OK` | returned and fully usable |
| `PARTIAL` | returned with some null entries |
| `REQUIRED_UNAVAILABLE` | a required variable is absent, wrong length or all-null |
| `OPTIONAL_UNAVAILABLE` | an optional variable is absent, wrong length or all-null |
| `MISSING` | the field was not present in the response |
| `ARRAY_LENGTH_MISMATCH` | the array length differs from the time axis |
| `NULL_ARRAY` | the field is present but every entry is null |

`UNAVAILABLE_STATUSES` groups every value except `OK` and `PARTIAL`.
`status.json.variable_model_policy` states that the models are independent and that
`cross_model_averaging` is `false`.

### New schema definitions

- `summary.schema.json` gains `$defs.deterministicCompact`, `$defs.ensembleCompact`,
  `$defs.viewingConditions`, `$defs.fogInputs`, `$defs.targetWindowSlot`,
  `$defs.targetWindowLocation` and `$defs.variableStatus`. The target-window slot
  exposes `ec_det`, `gfs_det`, `ec_ens`, `gefs`, `gfs_support`,
  `gfs_deterministic_vs_gefs`, `model_consistency` and `viewing_conditions` for the
  `MORNING`, `AFTERNOON` and `NIGHT` windows; each location also carries
  `fog_inputs`.
- `status.schema.json` gains `variable_status`, `variable_model_policy`,
  `target_coverage` and `target_dates`.
- `gefs.schema.json` gains `requested_variables`, `required_variables`,
  `optional_variables`, `variable_status` and `cloud_layer_status`.
- `module.schema.json` gains the same requested/required/optional split plus
  `variable_status`, so every module envelope reports availability the same way.

`viewing_conditions` judges the layers separately: low cloud is terrain obstruction,
mid cloud flattens direct light, high cloud adds sky texture and sunrise/sunset
potential. High cloud is explicitly **not** treated as bad weather. `fog_inputs`
publishes raw overnight indicators only — it never publishes a fog probability.

The main `history_comparison.json` is configured for
`history_years=[2023, 2024, 2025, 2026]`. It preserves the v1.0/v1.1 `2025`/`2026`
daily and metrics keys, and adds the older years plus `delta_2026_minus_2023`,
`delta_2026_minus_2024`, and `deltas_2026_minus`. The `same_grid_qa` object checks
every configured year; a returned-grid mismatch makes the comparison `FAILED` and no
historical delta is usable.

`history_forward.schema.json` defines the historical forward-path artifact. It uses
`history_years=[2023, 2024, 2025]`, anchors on the current `forecast_date`, and
exposes `d0_7`, `d8_15`, and `d16_to_11_01` under each core region and year. The last
window is hard-clipped at November 1, the end of the Jiuzhaigou target window. The
three historical records for each core point must pass the existing 13.5 km
Historical grid-distance QA and share one returned grid before
`cross_year_comparison_usable` can be true.

### Rolling-window applicability in `history_forward`

Because the rolling windows are clipped at a fixed calendar cutoff, they eventually
run past it. Once the anchor date advances far enough that `forecast_date + offset`
is after the cutoff, the window has no calendar days left: it is structurally empty,
not a fetch failure.

- `window_definitions[].status` becomes `NOT_APPLICABLE` with
  `reason = WINDOW_AFTER_CUTOFF`. A reader that only knows the older vocabulary may
  treat `NOT_APPLICABLE` as "this window does not exist in this season"; it never
  means missing data.
- Every per-year window (`$defs.window.status`) becomes `NOT_APPLICABLE` instead of
  `UNAVAILABLE`/`INVALID`, and the per-point `status` becomes `NOT_APPLICABLE`.
- Closed windows are excluded from the point-level OK/FAILED judgement, from
  `regions[].status`, and from `partial_points`. Only the still-applicable windows
  have to be `OK` for the module to report `OK`.
- When the anchor reaches November 2 every window is closed: the module reports
  `status = SKIPPED`, every point carries `usable_for_main_chain = false` and
  `reason = HISTORY_FORWARD_WINDOW_CLOSED`, `successful_fetches` / `failed_fetches` /
  `expected_fetches` are all `0`, and no HTTP request is issued.
- The lightweight views consumed by `phenology_weather_summary.json` and the compact
  region paths only speak `OK`/`PARTIAL`/`INVALID`/`UNAVAILABLE`; there a closed
  window is folded into `UNAVAILABLE` while its `reason` is preserved.

### ECMWF single runs require the full-horizon cycles only

`single_runs.json` compares consecutive ECMWF IFS initialisations. ECMWF does not
publish every cycle with the same horizon: `00Z` and `12Z` carry the full horizon
(about 240 h) while `06Z` and `18Z` are short runs (about 144 h / 6 days). A cycle
missing because the model never published it is a model property, not a data failure.

Each run reports `cycle_class` (`LONG` / `SHORT`) and `forecast_horizon_hours`, and
the region verdict requires only the long cycles:

| Region state | Condition |
|---|---|
| `OK` | every requested long cycle succeeded |
| `PARTIAL` | `SINGLE_RUN_LONG_CYCLE_PARTIALLY_DISTRIBUTED`; at least `SINGLE_RUN_MIN_REQUIRED_RUNS` (2) long cycles succeeded |
| `FAILED` | fewer than 2 long cycles succeeded (`SINGLE_RUN_LONG_CYCLE_UNAVAILABLE`) |

Short cycles are still fetched, reported through `short_run_count_requested` /
`short_run_count_available`, and compared when present;
`required_run_count_requested/available` reports the long-cycle tally, and
`target_reachable_by_short_runs` records whether a short cycle alone could have
reached the target timestamp.

### Long range separates the required horizon from the best-effort edge

`long_range.schema.json` describes the coarse GEFS (`ncep_gefs05`) background signal.
`requested_forecast_days` is `35` and the declared background lead range stays
`D16_D35`, but the last block `D34_D35` sits on the model edge: the daily run executes
before that cycle is disseminated, so the trailing block is best effort.

- `required_forecast_days` (`LONG_RANGE_REQUIRED_LEAD_END + 1`, currently `34`, i.e.
  lead days `D0`–`D33`) is what the artifact actually requires.
- `aggregation.required_lead_day_range` and `aggregation.edge_blocks` publish the split.
- `qa.long_range_horizon_check` reports `missing_required_lead_days` separately from
  `edge_shortfall_lead_days`, and from `missing_lead_days` (their union). Only a gap
  inside the required range can pull `forecast_horizon_status` down to `PARTIAL` or
  `FAILED`; a missing `D34_D35` block is recorded but does not fail the module.
- The same split applies per variable. `ncep_gefs05` routinely publishes
  `precipitation` and `snowfall` one 3-day block shorter than `temperature_2m`, so
  `long_range_member_check.edge_truncated_variables` is non-empty on most days. A
  variable only sets `variable_horizon_partial` (and downgrades the region) when its
  common complete range stops inside the required range — that is, when
  `first_timestamp` is after the forecast origin or `last_timestamp` is before lead
  day `LONG_RANGE_REQUIRED_LEAD_END`. Region QA publishes the deciding list as
  `variable_horizon_partial_variables` and the raw list as `edge_truncated_variables`.

`grid_registry.schema.json` describes the lightweight point-to-returned-grid audit
registry. `phenology_weather_summary.schema.json` describes the machine-readable
compact statistics file; it contains no hourly or daily raw series. Full audit data
remains in the module artifacts and compressed raw archives.

`gefs.schema.json` describes the independent Open-Meteo GEFS module. `ncep_gefs025`
is the near-range global product (about 0.25°, 31 sequences, approximately 10 days)
and `ncep_gefs05` is the coarse long-range global product (about 0.5°, 31 sequences,
approximately 35 days). The API currently provides 3-hour ensemble fields; a 3-hour
output frequency does not mean that a date more than 10 days away has 3-hour forecast
precision. The public artifact contains distributions, probabilities, event phase
statistics, and deterministic-support checks; member-level raw response data remains
in the compressed raw archive/cache.
