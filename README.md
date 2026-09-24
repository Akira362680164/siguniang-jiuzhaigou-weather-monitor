# siguniang-jiuzhaigou-weather-monitor

这是一个只使用 Open-Meteo 的、可审计的四姑娘山—九寨沟天气证据管道。它为下游分析提供固定坐标上的预报、历史 IFS 分析、集合成员、Single Runs、GFS 交叉验证、空间格点去重、天气事件风险数据，以及目标期简表和三年前后同期历史对照。它不自动给出“秋色提前/滞后几天”或最终黄度结论。

追踪窗口：

- 2026-10-24、2026-10-25：双桥沟、毕棚沟、理小路高位段；
- 2026-10-31、2026-11-01：九寨沟树正寨/诺日朗/原始森林/长海代表点。

## 下游入口（machine-readable entry points）

以下地址是公开 GitHub Raw URL，无需登录即可读取。日常优先读取 `status.json` 和轻量 `phenology_weather_summary.json`；`summary.json` 保留现有日报字段。

- status.json: <https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/status.json>
- summary.json: <https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/summary.json>
- target_summary.json: <https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/target_summary.json>
- hres.json: <https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/hres.json>
- history_comparison.json: <https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/history_comparison.json>
- historical_comparison.json: <https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/historical_comparison.json>
- history_forward.json: <https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/history_forward.json>
- ensemble.json: <https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/ensemble.json>
- gfs.json: <https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/gfs.json>
- single_runs.json: <https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/single_runs.json>
- spatial_sampling.json: <https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/spatial_sampling.json>
- long_range.json: <https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/long_range.json>
- gefs.json: <https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/gefs.json>
- phenology_weather_summary.json: <https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/phenology_weather_summary.json>
- weather_events.json: <https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/weather_events.json>
- grid_registry.json: <https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/grid_registry.json>

`phenology_weather_summary.json` 是轻量机器可读产物，沿用与上游同源管道的文件名和字段契约，不含逐小时/逐日原始数组。

## 职责边界

本仓库负责 GitHub Repository、GitHub Actions、Open-Meteo 请求、原始数据留存、QA、2026 与配置历史参考年的同点天气指标、稳定 JSON Schema、历史归档和机器可读输出。

下游分析负责每天读取 JSON，搜索并人工查看 2026/2025 同地点实拍，结合天气驱动力判断实际物候日差、用户到访日黄度和挂叶风险，并输出每日简报。JSON 中的 `weather_driver_vs_2023`/`weather_driver_vs_2024`/`weather_driver_vs_2025` 只代表天气驱动力，不是实际物候结论。

## 唯一数据源和模型

所有数值天气数据均来自 Open-Meteo；程序通过 endpoint 主机白名单阻止其他天气站点或 App 混入。相关官方文档：

- [ECMWF Forecast API](https://open-meteo.com/en/docs/ecmwf-api)：主模型为 ECMWF IFS HRES 9 km，endpoint 为 `https://api.open-meteo.com/v1/ecmwf`，请求 `forecast_days=15`。
- [Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)：endpoint 为 `https://archive-api.open-meteo.com/v1/archive`，固定 `models=ecmwf_ifs`。
- [Ensemble API](https://open-meteo.com/en/docs/ensemble-api)：川西使用全球 `ecmwf_ifs025_ensemble`，约 25 km、51 个成员；该模型官方预报长度为 15 天，当前模块请求 `forecast_days=15`。
- [Single Runs API](https://open-meteo.com/en/docs/single-runs-api)：endpoint 为 `https://single-runs-api.open-meteo.com/v1/forecast`，固定 `models=ecmwf_ifs`，比较不同 UTC 初始化 run。
- [GFS API](https://open-meteo.com/en/docs/gfs-api)：固定 GFS Global 0.11°（约 13 km），请求 `forecast_days=16`，只作趋势交叉验证。
- [Ensemble API](https://open-meteo.com/en/docs/ensemble-api) 与 [官方 Ensemble OpenAPI 注册表](https://github.com/open-meteo/open-meteo/blob/main/openapi/ensemble.yml)：16–35 天背景层当前使用全球 GFS Ensemble 0.5°，请求模型 ID 为 `ncep_gefs05`。
- 独立 GEFS 链同样使用 [Ensemble API](https://open-meteo.com/en/docs/ensemble-api)：近中期请求 `ncep_gefs025`（全球约 0.25°、约 25 km、31 个序列、当前约 10 天），远期请求 `ncep_gefs05`（全球约 0.5°、约 50 km、31 个序列、当前约 35 天）。两段分别保存和统计，不与 ECMWF Ensemble 平均。

所有请求统一使用：

- `timezone=Asia/Shanghai`
- `cell_selection=nearest`
- `elevation=nan`
- 固定请求坐标和 WGS84 纬经度

`elevation=nan` 用于禁止 Open-Meteo 的统计高程下推；返回的 `elevation` 是模型网格高程，不能当作用户输入的 DEM 或景区实测海拔。`cell_selection=nearest` 让返回格点选择规则固定且可审计。

主预报的 `precision_class` 按 ECMWF 原生分辨率解释：0–90 小时为 `native_hourly`，90–144 小时为 `coarse_3h_interpolated`，超过 144 小时为 `trend_only_6h_plus`。Open-Meteo 可能把较粗原生时间步插值为逐小时数组，因此逐小时返回值不等于全程原生逐小时预报。GFS 在 120 小时后也使用较粗时间步；Ensemble 按其约 3 小时原生序列理解。

## 统一天气变量体系（schema 1.4.0）

四套模型——ECMWF deterministic（HRES）、ECMWF ensemble、GFS deterministic、GEFS ensemble——现在请求并上报同一套变量：

| 类别 | 变量 |
|---|---|
| 温度 | `temperature_2m`、`dew_point_2m`、`relative_humidity_2m` |
| 降水 | `precipitation`、`rain`、`snowfall` |
| 云量 | `cloud_cover`、`cloud_cover_low`、`cloud_cover_mid`、`cloud_cover_high` |
| 风 | `wind_speed_10m`（持续风）、`wind_direction_10m`（风向，圆形量）、`wind_gusts_10m`（阵风） |
| 辐射 | `sunshine_duration`，不可用时回退 `shortwave_radiation` |

约束：

1. **数值只来自 Open-Meteo。** 不接入 Windy、Meteologix、天气 App 或县级天气网站。
2. **四套模型互相独立。** 只做逐变量比较，不做跨模型平均，也不把 ECMWF ensemble 与 GEFS 平均；确定性模型与集合模型同样不平均。
3. **分层云始终取自 API。** 绝不用 `总云量 − 低云` 反推中云/高云——三层相互重叠、不可相减。数据源返回全 `null` 数组时，该层显式标为 `OPTIONAL_UNAVAILABLE`，不用总云量顶替。
4. **required / optional 边界。** 核心必需变量不可用会使模块降为 `PARTIAL`/`FAILED`；可选变量不可用只记录状态，不影响模块整体可用性。GEFS 的分层云在川西当前返回全 `null` 数组，因此稳定落在 `OPTIONAL_UNAVAILABLE`（实跑见 `gefs.json.cloud_layer_status`）。
5. **阵风与持续风是两个量。** 所有产物中 `wind_gusts_10m` 与 `wind_speed_10m` 分开，不存在「把阵风当持续风」的字段。现有落叶机械应力阈值体系（核心约 gust ≥ 50 km/h）保持不变。
6. **风向按圆形量处理。** 逐小时保存，只用矢量平均（`atan2(Σsin, Σcos)`）归约；集合分布里不出现 `wind_direction_10m` 的算术百分位。退化时返回 `null`。
7. **不可用就是不可用。** 缺失值写 `null`（状态字段写 `UNAVAILABLE`），绝不填 `0`。若某变量不可用，只记录该变量不可用，不让整个模型模块失败（除非缺的是 required/core 变量）。

`status.json` 按模型分组发布 `variable_status`（`OK` / `PARTIAL` / `REQUIRED_UNAVAILABLE` / `OPTIONAL_UNAVAILABLE` / `MISSING` / `ARRAY_LENGTH_MISMATCH` / `NULL_ARRAY`）与 `variable_model_policy`（`independent=true`、`cross_model_averaging=false`）。

`summary.json.target_window_brief` 的每个窗口（`MORNING` 08:00–12:00、`AFTERNOON` 12:00–18:00、`NIGHT` 18:00–次日 08:00）同时给出 `ec_det`、`gfs_det`、`ec_ens`、`gefs` 四个紧凑视图、`gfs_support`、`model_consistency` 与 `viewing_conditions`：

- `deterministicCompact`：`cloud` / `low_cloud` / `mid_cloud` / `high_cloud`、`precip_mm` / `rain_mm` / `snow_cm`、`temp_min_c` / `temp_mean_c` / `temp_max_c`、`dew_point_c`、`relative_humidity_pct`、`wind_speed_kmh`、`wind_direction_deg`、`gust_kmh`、`sunshine_or_shortwave`。
- `ensembleCompact`：`cloud_median` / `low_cloud_median` / `mid_cloud_median` / `high_cloud_median`、`p_cloud_gt_50` / `p_cloud_gt_70`、`p_low_cloud_gt_50` / `p_mid_cloud_gt_50` / `p_high_cloud_gt_50`、`p_precip`、`p_snow`、`wind_speed_median`、`gust_median` / `gust_p90`、`temp_p10` / `temp_median` / `temp_p90`、`dew_point_median`、`relative_humidity_median`，以及 `layer_availability` 明确标示哪一层真的有数据。

`viewing_conditions` 分层判断摄影条件：`total_cloud_signal`、`low_cloud_signal`（山体遮挡/能见度）、`mid_cloud_signal`（直射光被压平）、`high_cloud_signal`（天空纹理与霞光潜力）、`precip_signal`、`snow_signal`、`wind_signal`、`visibility_related_signal`、`model_agreement`。**高云不等于坏天气**，只有在低云或降水同时存在时才降低判断等级。

**遮挡类别的定义边界（`visibility_related_signal`）**：只有低云、降水/雪雾和高湿环境能物理遮住山体，因此 `obstruction_inputs` 只取 `low` 与 `precip`。**中云不进遮挡判据**——中云压平直射光、影响平光质感，这件事由 `mid_cloud_signal` 单独表达。此前把中云并入遮挡会把「中云多」误报成「能见度风险高」。不使用 Open-Meteo 字段冒充能见度实测量。

**信号来源透明化（`signal_sources`）**：`layer_sources` 只说明三层云各自取自哪套集合，无法区分总云/降水/雪/风。`signal_sources` 为全部 7 个信号逐一标注**实际供数的那套集合**，取值为 `gefs` / `ecmwf_ensemble` / `UNAVAILABLE`：`total_cloud`、`precip`、`snow`、`wind` 优先取 GEFS；GEFS 不提供分层云时低/中/高云自动回落到 ECMWF 集合。**这是「优先取一套、缺失才回落」，不是两套集合的融合或平均**，`signal_sources` 就是为了让读 summary 的人不会误读成 EC+GEFS 融合值。

`fog_inputs`（沟谷晨雾辅助位）只发布原始指标：`previous_12h_precip_mm`、`previous_24h_precip_mm`、`night_relative_humidity`、`night_dew_point`、`night_temp`、`night_temp_dewpoint_spread`、`pre_dawn_wind_speed`、`pre_dawn_gust`、`night_total_cloud` / `night_low_cloud` / `night_mid_cloud` / `night_high_cloud`，以及 `moisture_signal`、`radiative_cooling_signal`、`wind_signal`、`system_low_cloud_risk`。`probability_published` 恒为 `false`——不输出「晨雾概率 73%」这类伪精确数字。

## 坐标注册表和主链闸门

`config/points.json` 是唯一坐标注册表（`namespace=siguniang_jiuzhaigou`）。只有 `status=VERIFIED` 的点能进入正式请求、历史差分和主链摘要；代码中的 `active_points()` 是硬过滤边界。

已启用的 VERIFIED 点：

- SQG_SHUANGQIAO 四姑娘山·双桥沟游客中心：`31.1035, 102.9239`（`subregion=shuangqiao`，`role=core`）
- BPG_CENTER 毕棚沟·游客中心：`31.3806, 102.9944`（`subregion=bipenggou`，`role=core`）
- LXL_HIGH_PASS 理小路·高位垭口代表点：`31.4, 102.84`（region 级 `route_high_point`，不归属景区子区）
- JZG_TREESHENG 九寨沟·树正寨/中段：`33.198939, 103.897435`（`subregion=shuzheng`，`role=route_mid`）
- JZG_NORILANG 九寨沟·诺日朗中心：`33.28, 103.906`（`subregion=rize`，`role=route_mid_high`）
- JZG_PRIMEVAL 九寨沟·原始森林：`33.347, 103.865`（`subregion=rize`，`role=route_high`）
- JZG_LONGHAI 九寨沟·长海：`33.036228, 103.932113`（`subregion=zezhawa`，`role=route_high`）

每个点位的 `coordinate_note` 写明其代表范围与局限：景区代表点不是气象站，沟内不同海拔会明显分异。`config/points.json` 的 `route_slots` 当前为空；未核实路段不应凭猜测填入。`grid_qa.hres_limit_km` 声明本申报点位的 HRES 格点代表性容差，取值依据见「QA 和失败机制」。

四姑娘山已注册为两个子区：`shuangqiao` 双桥沟（SQG_SHUANGQIAO）和 `bipenggou` 毕棚沟（BPG_CENTER）。九寨沟已注册为三个子区：`shuzheng` 树正沟（JZG_TREESHENG）、`rize` 日则沟（JZG_NORILANG、JZG_PRIMEVAL）和 `zezhawa` 则查洼沟（JZG_LONGHAI）。子区内部对 unique model grids 等权；`siguniang` composite 再对 shuangqiao 与 bipenggou 等权，`jiuzhaigou` composite 再对 shuzheng、rize、zezhawa 等权，均不按景点数量加权。理小路高位段只作为 region 级 core point，不进入任何景区子区，也不参与 composite。

`config/points.json` 的 `history_years` 为 `[2023, 2024, 2025, 2026]`。

### 分区与返回格点聚合

天气统计不把同一条沟内的多个点位当成等权独立样本。每个请求保存 requested coordinate、返回模式格点、高程、格点距离、时区和 QA；相同 `returned_grid_coordinate` 只保留一个独立样本，映射关系写在 `grid_registry.json` 以及轻量摘要的 `sampling` 中。

子区第一层对 unique model grids 等权平均；景区 composite 第二层对子区等权平均。当前配置的最低独立格点数（`minimum_verified_unique_grids`）为：双桥沟 1、毕棚沟 1、树正沟 1、日则沟 1、则查洼沟 1。这是按已核实的子区尺度和实际返回格点设置的，不把重复格点当成额外样本。任何子区低于自身门槛时，子区为 `PARTIAL`，景区 composite 也不会用其他子区替代或按点数加权。

## QA 和失败机制

每条记录都保留 source、endpoint、请求坐标、返回模型格点坐标、返回高程、timezone、UTC offset、retrieval time、模型/run 初始化时间（接口提供时）、格点距离和 QA 结果。QA 包含坐标、格点代表性、时区、模型、elevation 参数和数据完整性检查。

格点距离超过对应模型允许范围会标记 `GRID_REPRESENTATIVENESS_FAIL`，记录变为 `INVALID`，不进入天气差分指标。API 错误、超时、重试后失败、缺字段、缺数据、模型不符、时区不符和数组不完整也都标记 `INVALID`。每个请求最多 3 次、指数退避；重试仍只访问 Open-Meteo，没有天气源 fallback。`sunshine_duration` 若在同一个 Open-Meteo endpoint 被判定为不可用，只切换到同源 `shortwave_radiation`，并在 JSON 中记录实际变量。

HRES 主链的格点容差是**站点级**参数，不是全局常量。`config/points.json` 的 `grid_qa.hres_limit_km` 声明本点位允许的请求点—返回格点距离；未声明该键的站点仍回落到代码默认 `HRES_GRID_QA_LIMIT_KM = 14.0`，行为不变。之所以需要站点级覆盖：Open-Meteo `/v1/ecmwf` 实际按 **0.25° 网格**返回，容差上限即格点半对角线，而其东西向边长随纬度按 `cos(纬度)` 收缩（48°N 约 16.7 km、33°N 约 18.1 km）。14.0 km 是首届阿尔泰点位（48°N，最差点 13.81 km）定的初值；川西在 31–33°N 用同款网格天然更松，九寨沟原始森林（15.19 km）与理小路高位垭口（14.02 km）会被默认值误判。本仓库取 `16.5 km`，沿用 `sampling` 已有的容差口径（`radius_km` 12 + `grid_qa_extra_allowance_km` 4.5），即不宽于管道已经给自己的 12 km 空间样本的容差。每个记录的 `qa.grid_distance_limit_km` 仍逐条写明实际生效值。

即使 QA 失败，管道仍会写出 `status.json` 并把相应模块写成 `FAILED`；初始化级错误也会写最小失败状态。Actions 只有在测试或程序自身崩溃时失败，普通 API/QA 失败会保留产物供排错。

Open-Meteo 实际返回中，HRES 和 Single Runs 可能在数组边缘出现 `null`：程序只删除连续的首尾不完整行，并记录 `original_timestep_count`、保留行数和 `horizon_status=TRUNCATED_EDGE_MISSING`；中间缺失不会被填补，仍为 `INVALID`。普通 Forecast、Historical 和 Ensemble 响应未必提供模型初始化字段，JSON 会保留 `null`，而请求模型、endpoint 和返回格点仍完整记录；Single Runs 的 UTC 初始化时间由请求和记录显式保存。

## 历史差分和指标边界

本 namespace 的 2023、2024、2025、2026 均从 8 月 25 日累计到最近已完成的本地日期，使用同一固定请求坐标、同一 `ecmwf_ifs` 和同一时区。年份由 `config/points.json` 的 `history_years` 控制。它只能称为 `ECMWF IFS historical weather / analysis`，不是 `station observation`，因为它不是气象站实测。

每个地点计算：日最低/最高/平均温度、夜最低温、`<15℃`/`<10℃`/`<5℃`/`<2℃`/`<0℃` 寒夜累计、各阈值连续寒夜序列及最大连续长度、昼夜温差、降水、降雪、云量、低云、日照/短波、平均风和最大阵风。

`history_comparison.json` 保留同点同模式格点 QA 与 `delta_2026_minus_2025`，并同时给出 `delta_2026_minus_2023`、`delta_2026_minus_2024` 和通用 `deltas_2026_minus`。每个年份的 `daily` 与 `metrics` 都完整保留；所有配置历史年份返回格点必须一致，否则该点比较为 `FAILED`，不生成混格点差分。主日报继续使用 `weather_driver_vs_2025`，并可读取 `weather_driver_vs_2023`、`weather_driver_vs_2024`。

`coldness_index` 是本项目的内部相对比较指标，不是官方物候模型，公式为每个完整日累加：

```text
max(0, 10 - daily_mean)
+ 2 * max(0, 5 - night_min)
+ 3 * max(0, 2 - night_min)
+ 4 * max(0, 0 - night_min)
```

`weather_driver_vs_2025.direction` 只允许 `LEADING`、`SYNC`、`LAGGING`、`UNDETERMINED`，表示天气指标相对 2025 的方向，不生成 `actual_phenology_lead_days`。

### 历史年份同期后续天气路径

`data/latest/history_forward.json` 以当天 `forecast_date` 为 anchor，调用 Historical Weather API 查询 2023、2024、2025 同一日历节点之后真实发生的天气；2026 仍由现有实时预报链提供，不进入该历史模块。

历史 API 使用持久化缓存：`data/cache/history/<namespace>/<year>/<point_id>.json`。缓存只保留紧凑的逐日值，但完整绑定请求坐标、返回模式格点、返回高程、模型参数、`cell_selection=nearest`、`elevation=nan`、时区、endpoint 和 QA。首次运行会补齐当前需要的历史日期；后续运行由 `history_comparison`、`history_forward` 和 Long Range 的历史参考层在本地切片，只有缓存缺失的连续日期才重新请求 Open-Meteo。缓存身份不一致会标记 `INVALID`，不会混用不同坐标、模型或格点。运行记录会在模块的 `history_cache` 字段中给出命中数、补抓数和实际 API 请求数。

需要偶尔重新核验时，可在本地使用 `python src/pipeline.py --refresh-history`，或从 GitHub Actions 的 `workflow_dispatch` 表单勾选 `refresh_history`。强制复核仍只接受通过同一 QA 的 Open-Meteo 返回；验证失败会保留失败状态，不会用旧缓存或其他来源伪装成最新数据。

每个 VERIFIED 核心区域提供 `regions.<region>.years.<year>.d0_7`、`d8_15` 和 `d16_to_11_01` 三个窗口。以 2026-09-02 为例，三个窗口分别是 09-02/09-09、09-10/09-17、09-18/11-01，均包含首尾；日期随每日 anchor 滚动，11-02 及以后永远不请求、不写入输出。窗口同时保留每日温度、降水、降雪、日照和最大阵风，以及窗口统计和前 3 日/后 3 日平均温度变化。历史年份的 `d16_to_11_01` 也会被截断到各年的 `11-01`。

窗口可用性独立判断：`d0_7.status=OK` 且 `usable_for_main_chain=true` 时进入当前主链；`d8_15.status=PARTIAL` 时保留已完成日期并将 `usable_for_trend_reference=true`；`d16_to_11_01.status=INVALID` 时 `usable_for_main_chain=false`，不参与当前结论。单个窗口缺失不会把整个地区标记为 `usable_for_main_chain=false`；地区级可用性以当前 `d0_7` 是否可用为准。

滚动窗口最终会撞上固定的 11-01 截止。当 anchor 前进到 `anchor+offset` 已越过 11-01 时，该窗口没有任何日历日，属于**结构性不存在**而不是抓取失败：`window_definitions[].status` 写为 `NOT_APPLICABLE`（`reason=WINDOW_AFTER_CUTOFF`），逐年同名窗口与对应点状态也写为 `NOT_APPLICABLE`，并从点级判定、`regions[].status` 和 `partial_points` 中排除——只有仍然适用的窗口才要求 `OK`。例如 anchor 为 2026-10-17 时 `d16_to_11_01` 已关闭，但模块仍回到 `OK`；当 anchor 到 11-02、三个窗口全部关闭时，模块写 `SKIPPED`，每个点 `usable_for_main_chain=false`、`reason=HISTORY_FORWARD_WINDOW_CLOSED`，`successful_fetches`/`failed_fetches`/`expected_fetches` 均为 0，且不发出任何 HTTP 请求。轻量视图（`phenology_weather_summary.json`、紧凑区域路径）只有 `OK`/`PARTIAL`/`INVALID`/`UNAVAILABLE` 词表，关闭窗口在那里折叠为 `UNAVAILABLE` 并保留原 `reason`。

每个核心点的 `same_grid_qa` 会检查 2023、2024、2025 的请求坐标、返回模式格点、返回高程、格点距离、时区、模型和 API request metadata。三年返回格点完全一致且每年 QA 通过时，`cross_year_comparison_usable=true`；任何年份失败、格点不一致或超过现有历史格点距离限制时，点和区域标记为 `FAILED`，不进入跨年比较。四姑娘山的 shuangqiao/bipenggou 与九寨沟的 shuzheng/rize/zezhawa 都执行相同三年同格点闸门，只有全部子区均通过后才形成对应的 `siguniang`/`jiuzhaigou` composite。

`history_forward.json` 额外提供 `regions.<region>.subregions.<subregion>.years.<year>.<window>` 和 `regions.<region>.composite.years.<year>.<window>`。2023、2024、2025 的历史路径与 2026 的 HRES 预报使用同一注册点集合、同一返回格点去重算法和同一两级聚合规则；不同 API 产品的网格坐标本身不强行要求相同。只有历史参考年之间的 `same_grid_qa` 通过后，历史跨年聚合才可用。

该文件只提供历史天气路径证据，不输出物候、秋色或旅游结论。它与 `history_comparison.json` 并行存在，不改变 HRES、ECMWF Ensemble、GFS、Single Runs、Spatial Sampling 或 Long Range 的逻辑。

## Single Runs 初始场比较

`data/latest/single_runs.json` 用 `models=ecmwf_ifs` 比较相邻 UTC 初始化 run，用于判断最新一次预报相对上一次是否发生漂移。ECMWF IFS 并不是每个 cycle 都发布同样长度：`00Z`/`12Z` 是全长跑（约 240 h），`06Z`/`18Z` 是短跑（约 144 h，6 天）。**模型没有发布某个 cycle 属于模型属性，不是数据失败**，因此每条 run 会记录 `cycle_class`（`LONG`/`SHORT`）与 `forecast_horizon_hours`，区域判定只要求长跑：

| 区域状态 | 条件 |
|---|---|
| `OK` | 所有请求到的长跑全部成功 |
| `PARTIAL` | `status_reason=SINGLE_RUN_LONG_CYCLE_PARTIALLY_DISTRIBUTED`，成功长跑数 ≥ `SINGLE_RUN_MIN_REQUIRED_RUNS`（2） |
| `FAILED` | 成功长跑数 < 2（`SINGLE_RUN_LONG_CYCLE_UNAVAILABLE`） |

短跑仍会正常请求、计数（`short_run_count_requested`/`short_run_count_available`）并在存在时参与比较；`target_reachable_by_short_runs` 记录目标时刻是否本来就只有短跑能覆盖，`required_run_count_requested/available` 给出长跑口径的完整计数。

区域 `target_time` 优先取该 region `primary_visit_date` 的当地 05:00：四姑娘山与理小路为 2026-10-24，九寨沟为 2026-10-31。当目标日尚未进入 10 天 run 视界时，退回 `now + 3 天` 的可审计滚动目标时刻，并在 `target_policy` 中记录采用的策略。

## 空间采样、集合和风雪风险

`siguniang`、`lixiaolu`、`jiuzhaigou` 对每个 VERIFIED 核心点请求核心 + N/S/E/W/NE/NW/SE/SW 约 12 km 的同源 HRES 样本；程序保存每个请求坐标和返回格点坐标，并按返回格点去重。`requested_samples=9` 不等于 9 个独立模式样本；输出 `unique_model_cells`、重复请求数、核心区温度范围和按日期/唯一格点的 `cold_pool_coverage`，用于区分广泛覆盖与单格点现象。

Ensemble 仅对每个 region 的核心点（`core_point_id`）计算 51 成员的 mean、median、p10、p25、p75、p90、spread，以及夜最低温 `<5℃`、`<2℃`、`<0℃` 的成员支持比例。约 25 km Ensemble 只表达信号稳健性，不能当作沟内精确温度，也不与 HRES 简单平均。

## 16–35 Day Long-Range Background

`data/latest/long_range.json` 是长期背景层。当前实际核验的 Open-Meteo GFS Ensemble 配置为：`model_id=ncep_gefs05`、31 个序列（基准/控制序列加 `member01`–`member30`）、全球覆盖、约 0.5°（约 50 km）、原生 3 小时。官方参数表列 `forecast_days` 为 0–35（`forecast_days=36` 会被接口按 `Allowed range 0 to 36` 这类边界规则拒绝），因此 `requested_forecast_days=35`；模块仍会对返回的本地日期数和非空 lead day 再做 QA。如果接口以后不再接受该请求，模块会写 `FAILED`，不会换用其他天气源。

必需交付区间与边缘块分开：声明的背景层覆盖 `D16_D35`，但最后一块 `D34_D35` 位于模型边缘——每日运行发生在该 cycle 分发之前，尾块只能尽力而为。`required_forecast_days`（当前 `34`，即 lead `D0`–`D33`）才是模块真正要求的范围，`aggregation.required_lead_day_range` 与 `aggregation.edge_blocks` 公布这一划分。`qa.long_range_horizon_check` 把 `missing_required_lead_days` 与 `edge_shortfall_lead_days` 分开记录（`missing_lead_days` 是两者并集）；只有必需区间内出现空洞才会把 `forecast_horizon_status` 降为 `PARTIAL`/`FAILED`，缺 `D34_D35` 只记录、不失败。

变量级也按同一口径：`ncep_gefs05` 的 `precipitation` 和 `snowfall` 经常比 `temperature_2m` 少最后一块，因此 `long_range_member_check.edge_truncated_variables` 在多数日子都非空。只有变量的共同完整区间**落进必需区间内**（`first_timestamp` 晚于 forecast origin，或 `last_timestamp` 早于 lead `D33`）才算异常；区域 QA 用 `variable_horizon_partial_variables` 记录判定列表、用 `edge_truncated_variables` 记录原始列表。

该层按固定 3 天块聚合为 D16–D18、D19–D21、D22–D24、D25–D27、D28–D30、D31–D33、D34–D35，不在公开长期文件中展示逐小时成员数组。Open-Meteo 可能把集合原生 3 小时序列插值为逐小时数组，因此这些数组只用于内部聚合和审计，不能被解释为逐小时精确预报。

真实实跑中接口返回的本地日期数长期落在 34–35 天，温度非空值通常到 D33–D34，`D34_D35` 窗口常被写为 `UNAVAILABLE`；降水、降雪和阵风还会有各自更短的非空边界。按现行口径，只要 `D0`–`D33` 连续且无空洞，`forecast_horizon_status` 即为 `PASS`，缺尾块只进入 `edge_shortfall_lead_days`；每个变量的 `variable_availability` 仍会写入 QA。这是接口当前可用边界的记录，不是补值或伪造的 D35 预报。

长期温度方向使用同一点、同一时区和 `ECMWF IFS 9 km historical weather / analysis` 的 `historical_reference`。这是有限的同日期历史参考，不称为 `climatological_normal`，也不是气象站实测。

每个窗口输出集合分布、相对参考方向、集合支持的冷空气/降水/降雪/强风背景信号、粗网格阈值信号和不确定性。`coarse_grid_threshold_signal.usable_for_local_absolute_temperature=false`；0.5° 网格的 `<5℃`、`<2℃`、`<0℃` 只可作背景信号，不能直接证明沟内温度或霜冻。`wet_snow_assessment` 仅在粗网格降雪与 `<=2℃` 日最低温重合时标记 `COARSE_POTENTIAL`，不判断当地雪相或积雪量。

GitHub 每日运行会从最近 3–5 次长期摘要中比较同一 `horizon_class`，并标记 `NEW`、`PERSISTENT`、`STRENGTHENING`、`WEAKENING`、`SHIFTING` 或 `DISAPPEARED`。第一次运行没有前序摘要时为 `INSUFFICIENT_HISTORY`。紧凑的长期摘要随每日 archive 保留；包含成员级小时数据的 `raw/long_range.json.gz` 只保留 14 天。

下游分析可以用 16–35 天层提前关注 10 月中下旬前后的持续偏冷、冷空气重复、雨雪/湿雪背景、强风背景和集合是否收敛。它不能用这一层给出某日精确最低温或降水量、沟内霜冻、实际物候提前/滞后天数，也不能覆盖已经进入 8–15 天窗口的短周期证据。优先级固定为：

```text
0–7天：ECMWF HRES > ECMWF Ensemble > GFS
8–15天：ECMWF HRES趋势 + ECMWF Ensemble > GFS
16–35天：GFS Ensemble background only
```

如果长期背景层与后续进入 8–15 天的 HRES/ECMWF Ensemble 发生变化，以新的短周期模型为准。长期层也不把强风自动解释为掉叶；`leaf_loss_weather_risk` 仍只是天气事件风险，实际挂叶判断由下游结合实拍和成熟度完成。

## Independent GEFS and Target Window Brief

`data/latest/gefs.json` 是独立的 NOAA GFS Ensemble（GEFS）证据层，用来判断 GFS deterministic 的远期轨迹是否得到成员支持、天气过程大致落在哪个日期以及相位分歧有多大。它不替代 `gfs.json`，也不与 ECMWF Ensemble 做平均。

- `near_range` 使用 `ncep_gefs025`：全球约 0.25°、约 25 km、31 个序列，当前接口约 10 天；用于近中期成员分布和确定性 GFS 交叉验证。
- `long_range` 使用 `ncep_gefs05`：全球约 0.5°、约 50 km、31 个序列，当前接口约 35 天；用于 11 天以后到 `2026-11-01` 的趋势、概率和过程窗口。粗网格结果不能当作沟内小时级精准预报。
- GEFS 状态按必需/可选变量分层：`temperature_2m`、`precipitation`、`snowfall`、`cloud_cover` 和 `wind_gusts_10m` 是当前可用核心；低/中/高云层、相对湿度、平均风和日照变量按可选能力记录。当前真实接口在川西返回的 `cloud_cover_low/mid/high` 为空数组/空成员序列，`shortwave_radiation`/`sunshine_duration` 也可能不可用；这些缺失只进入 `optional_missing_variables` 和 warning，不会把核心完整的点误判为 `PARTIAL`。如果核心字段、成员数或时间轴失败，点才会进入 `PARTIAL`/`FAILED`。任何缺失都不会用其他平台补值。
- 模块级 `optional_unavailable_variables` / `required_unavailable_variables` 与逐段 `variable_status` 使用**同一套能力探测**（`unavailable_variables_for`，只把状态不在 `{OK, PARTIAL}` 的变量计为不可用），因此模块级列表永远不会和 `variable_status` 里的 `OPTIONAL_UNAVAILABLE` 条目互相打架。变量若完全不在 `variable_status` 里就不会被列出——这是能力探测的边界，不是遗漏。
- 每个窗口保留 temperature/cloud/low-cloud/precipitation/snowfall/gust 的百分位与概率，分母是 `members_valid`。成员缺失不会被当作零。
- `CLOUD_EVENT`、`PRECIP_EVENT`、`SNOW_EVENT` 和 `COLD_EVENT` 只表示天气过程候选；输出 `event_start/event_peak/event_end` 的 p25/median/p75、最早/最晚、`phase_spread_hours`、`phase_confidence`、`multimodal` 和 `event_day_distribution`。这些字段不表示物候阶段、黄叶或掉叶。
- `MORNING`、`AFTERNOON`、`NIGHT` 均按 `Asia/Shanghai` 聚合；11/1 的 NIGHT 会因硬截止只包含 11/1 当天 18:00 后的数据，不读取 11/2。
- `summary.json.target_window_brief` 是给下游日报快速读取的 2026-10-24 至 2026-11-01 简表。它只列 VERIFIED 点，按 `days[date][point_id]` 提供 HRES/GFS deterministic、ECMWF Ensemble、GEFS、天气过程相位、deterministic support、EC/GEFS consensus、`viewing_conditions` 和固定 `itinerary_focus`；这里只保留百分位/概率和窗口结论，不复制成员数组、完整 event distribution、请求元数据或 debug 结构。完整细节仍在 `gefs.json`、`ensemble.json`、`hres.json` 和 `gfs.json`。ECMWF Ensemble 的 D8–15 数据可以进入对应日期窗口；旅行简表仍硬截止于 11/1，因此 11/2 不进入该简表。
- ECMWF 集合只按**每个区域的核心点**抓取。非核心的简表节点（如 `JZG_NORILANG` 诺日朗、`JZG_PRIMEVAL` 原始森林）不报 `unavailable`，而是借用本区域核心格点并在 `ensemble_reference_point_id` 里写明借的是谁（例：`"JZG_TREESHENG"`）；核心点自身该字段为 `null`。含义是「本点的集合判断采用区域核心格点」——ECMWF 集合 25 km 网格下这几个点本就落在同一格点，重复请求没有信息增益。字段非 `null` 不代表该格点一定有数据，是否真有覆盖仍看 `ec_ens.available`。
- `window_overview.highest_snow_risk_windows` 只由 `snow_signal == "HIGH"` 决定，**不读 `precip_signal`**；降水风险另由 `precip_window` 与逐点 `precip_signal` 表达。`window_overview.highest_low_cloud_risk_windows` / `highest_mid_cloud_flat_light_windows` / `low_visibility_related_risk_windows` 则分别对应低云遮挡、中云平光和遮挡类别（低云+降水）三个互不含混的字段。

跨集合状态严格区分：`HIGH`/`MEDIUM`/`LOW` 只表示 ECMWF Ensemble 与 GEFS 都有有效覆盖时的跨模型比较；`ONE_ENSEMBLE_ONLY` 表示只有一套集合有覆盖，`UNAVAILABLE` 表示两套集合都不可用。ECMWF Ensemble 超出预报时效不会被当作 `LOW` 分歧。`window_overview.largest_model_disagreement_dates` 只收录真实 `LOW`，`single_ensemble_only_dates` 单独列出只有一套集合覆盖的日期。

时间尺度纪律：0–7 天可以看细观景窗口和集合分布；8–15 天以日期/过程为主，上午/下午只是低置信参考；16 天以后只读趋势、概率、过程窗口、相位离散度和风雪背景。3 小时数组的输出频率不等于 10 天以后拥有 3 小时预报精度。

主链优先级仍为：`0–7 天 ECMWF HRES > ECMWF Ensemble > GFS`；`8–15 天 ECMWF HRES 趋势 + ECMWF Ensemble > GFS`；`16–35 天 GFS Ensemble background only`。GEFS 请求只对 `VERIFIED` 点进入正式 `gefs` 与 `target_window_brief`；`PROVISIONAL`、`ROUTE_NOT_VERIFIED` 仍只会出现在排除/QA 信息中。

GFS 只输出 EC/GFS 的温度趋势、寒冷窗口、降水和强风一致性，不参与平均，也不直接产生秋色判断。`leaf_loss_weather_risk` 只表达强阵风、湿雪、雨雪和冻结等天气事件风险；9 月 20 日前强风不额外加权，9 月 20 日后才启用季节权重（固定日历阈值，与上游同源管道一致）。它不表示树叶一定掉落，实际挂叶风险由下游结合实拍和成熟度判断。

## Weather Events / Wind-Snow-Rain / Leaf Mechanical Stress

`data/latest/weather_events.json` 是天气事件数据库。历史部分只消费已经通过 QA 的 `data/cache/history/<namespace>/<year>/<point_id>.json`，不会为派生事件再次请求 Historical API；预报部分只消费本次运行的 HRES。历史 daily 与预报 daily 分别标记为 `source_state=finalized_history` 和 `source_state=forecast`，预报不会晋升为固定历史。预报衍生值硬截止到 `2026-11-01`。

每个完整日保留冻结、降雨、降雪、阵风阈值及组合事件 flag，并生成稳定的 `source_fingerprint`。派生缓存位于 `data/cache/weather_events/<namespace>/<year>/<point_id>.json`，绑定历史缓存的坐标、返回格点、模型、`elevation=nan`、时区和 QA。缓存命中时不改写文件；只对新增日期、源 fingerprint 变化或被移除日期更新。`cache_update` 与模块顶层 `weather_event_cache` 记录命中、回填、重算和源日期变化数量。

窗口统计对连续天气变量沿用现有 unique returned model grid 等权规则；阵风极值取有效 unique grid 的最大值，并记录来源 point/grid。事件统计同时给出 `any_grid_event`、触发的 unique grid 数和总 unique grid 数，不能把重复请求坐标当成独立样本。`siguniang` 按 shuangqiao/bipenggou 两子区等权，`jiuzhaigou` 按 shuzheng/rize/zezhawa 三子区等权，PROVISIONAL 点不会进入主链。

`cooling_episode_candidates` 是固定规则的天气降温过程候选，不是物候阶段。当前 `cooling_episode_v1` 参数为：连续完整日、前 3 日均温基准；当前日均温较基准至少下降 `3.0°C` 或夜最低温至少下降 `2.0°C` 才触发；后续日均温或夜最低温回到基准减 `0.5°C` 以内即结束；最多向后检查 3 日回暖，候选间至少间隔 2 日。小于这些阈值的日常波动不生成候选。所有 episode 只输出天气指标和 `rule_version`。

`mechanical_leaf_stress` 是天气机械压力分级，不是实际落叶概率，也不是任何树种的生物学硬阈值：阵风 `>=50 km/h` 记 `strong_wind`，`>=65 km/h` 记 `very_strong_wind`；雨雪与阵风组合分别记 `wind_plus_rain`/`wind_plus_snow`；强冻与雪组合记 `hard_freeze_plus_snow`。单日达到 `>=65 km/h` 或强冻加雪/强风加雪时为 `HIGH`；单日阵风 `>=50 km/h`、雨、雪或冻结但未达到 HIGH 时为 `MEDIUM`；有可用天气值但未触发组合时为 `LOW`。这层不写入树叶成熟度、不判断大量掉叶；下游仍需结合实拍判断。

## 行程面向产物

除上述模块产物外，本仓库额外发布两个直接对应本次行程的产物（均为本 namespace 专有）：

- `data/latest/target_summary.json`：按 `target_groups`（`siguniang_2026_10_24_25`、`jiuzhaigou_2026_10_31_11_01`）逐目标日、逐点给出温度、降水、云量和来源可用性，并标记每个目标日的覆盖等级。`status.json.target_coverage` 是同一信息的紧凑视图，覆盖等级为：
  - `HRES_AVAILABLE`：ECMWF HRES 已覆盖，作为近期期预报参考；
  - `GFS_AVAILABLE`：GFS 已覆盖；
  - `ECMWF_ENSEMBLE_AVAILABLE`：只有 ECMWF 集合进入窗口；
  - `GEFS_LONG_RANGE_AVAILABLE`：只进入 GEFS 0.5° 远期范围，只看温度/降水/云量趋势和集合分歧；
  - `NOT_YET_AVAILABLE`：尚未进入当前模型窗口，保持空值。
- `data/latest/historical_comparison.json`：以目标日期块为中心，向前后各取 3 个日历日，用 Open-Meteo Historical Weather API 的再分析格点对照 2023、2024、2025 年已发生天气，回答“去年是否也这样”和“目标日前后是否只是单日异常”。窗口为四姑娘山每年 10/21—10/28、九寨沟每年 10/28—11/4（`years=[2023,2024,2025]`、`window_days_each_side=3`）。它不是景区实测站，也不能替代当前年份的临近预报。

`historical_comparison.json` 的历史逐日记录另外按当地时间拆成 `00:00—06:00`、`06:00—12:00`、`12:00—18:00`、`18:00—24:00` 四段，分别统计总降水、液态雨、降雪、降水小时数和云量。分段依据接口返回的本地时间标签；Open-Meteo 的小时降水是此前一小时的累计值，因此边界时刻的降水归入其返回时间所在的时段，适合判断“偏夜间/上午/下午”的历史倾向，不应当解释成分钟级起止时间。

`target_summary.json` 与 `historical_comparison.json` 只提供机器可读天气事实，不输出物候、秋色或旅游结论。

## 目录和保留策略

```text
.
├── .github/workflows/update-weather.yml
├── config/points.json
├── data/
│   ├── cache/history/<namespace>/<year>/<point_id>.json
│   ├── cache/weather_events/<namespace>/<year>/<point_id>.json
│   ├── cache/gefs/<model_id>/<point_id>.json
│   ├── latest/
│   │   ├── status.json
│   │   ├── summary.json
│   │   ├── target_summary.json
│   │   ├── hres.json
│   │   ├── history_comparison.json
│   │   ├── historical_comparison.json
│   │   ├── history_forward.json
│   │   ├── ensemble.json
│   │   ├── gfs.json
│   │   ├── single_runs.json
│   │   ├── spatial_sampling.json
│   │   ├── long_range.json
│   │   ├── gefs.json
│   │   ├── grid_registry.json
│   │   ├── phenology_weather_summary.json
│   │   └── weather_events.json
│   └── archive/YYYY-MM-DD/
│       ├── 同名压缩后的每日 JSON（含 history_forward.json 和 phenology_weather_summary.json）
│       └── raw/*.json.gz
├── schemas/{status,summary,module,history_cache,weather_events_cache,weather_events,history_forward,historical_comparison,target_summary,long_range,gefs,grid_registry,phenology_weather_summary}.schema.json
├── src/pipeline.py
├── tests/test_pipeline.py
├── requirements.txt
└── README.md
```

`latest/` 保存完整数据；每日 archive 保存去掉逐小时数组的可读快照，`archive/YYYY-MM-DD/raw/` 保存压缩后的模块原始快照。原始 gzip 目录保留 14 天，紧凑每日快照、GEFS response cache 和派生 weather-event cache 长期保留。Schema 版本目前为 `1.4.0`。这是对 v1.0.0–v1.3.0 的兼容性新增：已有字段和模块语义保持不变，四套模型改用统一天气变量集合，`summary.json` / `status.json` / `gefs.json` / `module.json` 新增 `variable_status` 等可用性字段。读取旧 archive 时缺失字段按不可用处理，不做回填。破坏性变更必须升级 major version 并同步更新 Schema、测试和 README。

`schemas/README.md` 记录各 schema 的字段契约、本仓库专有的 trip-facing artifacts（`target_summary`、`historical_comparison`）以及通知下游的兼容策略。

## GitHub Actions 和本地运行

`.github/workflows/update-weather.yml` 支持 `workflow_dispatch`，并在 `00/06/12/18 UTC` 模型时次后第 17 分钟运行，即北京时间每天 `02:17、08:17、14:17、20:17`。17 分钟偏移用于避开整点负载；GitHub scheduled workflow 仍可能排队延迟，日志会保留名义触发与实际运行时间。它使用 Python 3.12、安装 `requirements.txt`、先运行单元测试，再读取/补抓 Open-Meteo 历史缓存和请求其他模块，随后从历史缓存增量构建 weather-event cache，最后在 `permissions: contents: write` 下提交 `data/latest`、`data/archive` 和 `data/cache`。手动触发时可勾选 `refresh_history`，强制重新核验历史缓存；派生 weather-event 层不会因此重复调用同一 Historical API。

本地运行：

```bash
python3.12 -m pip install -r requirements.txt
python3.12 -m unittest discover -s tests -v
python3.12 src/pipeline.py
# 偶尔强制复核历史缓存
python3.12 src/pipeline.py --refresh-history
```

运行日志会打印类似 `[SQG_SHUANGQIAO] HRES FETCH OK`、`[SQG_SHUANGQIAO] GRID QA PASS`、`[JZG_TREESHENG] HISTORY_FORWARD d0_7 OK` 和 `[BPG_CENTER] HISTORY_FORWARD SUBREGION OK`。JSON Schema 文件位于 `schemas/`，机器端应先检查 `status.json`，再按模块状态读取 `summary.json` 或相应明细。

## 推荐的下游每日读取顺序

1. 读取 `status.json`，确认 `pipeline_status` 和 `modules`；任何 `FAILED` 模块都按缺失证据处理。
2. 日常读取 `phenology_weather_summary.json`，按 `regions` 读取 `siguniang`（shuangqiao/bipenggou + composite）、`lixiaolu`、`jiuzhaigou`（shuzheng/rize/zezhawa + composite）的 2023–2026 窗口统计；该文件不含 hourly/daily 原始数组，并包含轻量 weather-event 字段。
3. 读取 `summary.json`，按 `regions` 的 `visit_date` 映射 10/24 双桥沟—毕棚沟—理小路、10/25 理小路—九寨沟方向、10/31 九寨沟树正沟—日则沟—则查洼沟、11/1 返程或二次入园的天气。
4. 用 `weather_driver_vs_2023`/`weather_driver_vs_2024`/`weather_driver_vs_2025`、`forecast_0_7d`、`forecast_8_15d`、`forecast_16_35d`、Ensemble 分布、Single Runs 和 GFS 交叉验证整理天气证据；长期层只作 16–35 天背景概率层，读 `required_forecast_days` 与 `edge_shortfall_lead_days` 区分必需区间和边缘块；需要查看完整历史同期后续路径时读取 `history_forward.json`，先检查其 `status`（`NOT_APPLICABLE`/`SKIPPED` 表示滚动窗口已越过 11-01 截止，不是数据缺失）、各 region 的 `subregion_aggregation_status` 和 `same_grid_qa`；读 `single_runs.json` 时以 `cycle_class=LONG` 的 run 为准。
5. 对需要结论的同地点，另行搜索并人工查看 2026/2025 实拍；把实拍判断与天气证据分开写，不能把 JSON 的天气方向改写成自动物候日差。
6. 读取 `weather_events.json`、`grid_registry.json`、`long_range.json`、`hres.json`、`history_comparison.json`、`historical_comparison.json`、`ensemble.json`、`single_runs.json` 追溯具体点、格点、成员和 run；遇到 `INVALID`、`FAILED`、`PARTIAL` 或 `UNDETERMINED` 时保留不确定性。weather events 只能用来描述天气事件和机械天气压力，不能直接改写为实际物候日期。
7. 判断摄影/通行条件时读取 `summary.json.target_window_brief`：每个窗口的 `ec_det`、`gfs_det`、`ec_ens`、`gefs` 四套独立视图、`model_consistency`、`viewing_conditions`（含逐层来源 `layer_sources` 与逐信号来源 `signal_sources`）和 `fog_inputs`。低云看山体遮挡、降水看遮挡、中云看直射光、高云看天空纹理；高云不等于坏天气。`signal_sources` 说明每个信号究竟取自 GEFS 还是 ECMWF 集合，看到它就不要把单个信号读成两套集合的融合值。差异大或某变量 `null` 时按「未确认」处理，不做跨模型平均。

当前 v1.4.0 Schema 已覆盖长期背景层、派生 weather-event 层、独立 GEFS 层、统一天气变量可用性（`variable_status`）、目标期简表和三年同期历史对照。后续如果需要增加图像人工复核结果，建议以独立字段或独立文件追加，并保持天气层与视觉判断层分离。

资料来源：

- [Open-Meteo Forecast API](https://open-meteo.com/en/docs)
- [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)
- [Open-Meteo Ensemble API](https://open-meteo.com/en/docs/ensemble-api)
- [Open-Meteo Single Runs API](https://open-meteo.com/en/docs/single-runs-api)
- [Open-Meteo GFS API](https://open-meteo.com/en/docs/gfs-api)

天气、景区开放、道路、景交和住宿仍需在临行前按当次证据复核。
