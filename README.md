# 四姑娘山—九寨沟秋季天气追踪模型

这是参考 [`altay-autumn-monitor`](https://github.com/Akira362680164/altay-autumn-monitor) 建立的独立天气证据管道，专门追踪：

- 2026-10-24、10-25：双桥沟、毕棚沟、理小路高位段；
- 2026-10-31、2026-11-01：九寨沟树正寨/诺日朗/原始森林/长海代表点。

项目只使用 Open-Meteo，固定 `Asia/Shanghai` 时区和 WGS84 坐标，并记录每次请求的返回格点、返回海拔、模型运行、格点距离和数据完整性。它追踪的是模型趋势，不把景区代表点写成现场气象站。

## 当前数据入口

推送到 GitHub 后可直接查看：

- [status.json](https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/status.json)：本轮状态和目标日是否已经进入模型窗口；
- [summary.json](https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/summary.json)：按目标日期的压缩摘要；
- [target_summary.json](https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/target_summary.json)：逐点温度、降水、云量和来源可用性；
- [hres.json](https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/hres.json)：ECMWF HRES 近 15 日；
- [gfs.json](https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/gfs.json)：NOAA GFS 近 16 日；
- [ensemble.json](https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/ensemble.json)：ECMWF 51 成员集合；
- [gefs.json](https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/gefs.json)：NOAA GEFS 0.25°近程与 0.5°远期集合。
- [historical_comparison.json](https://raw.githubusercontent.com/Akira362680164/siguniang-jiuzhaigou-weather-monitor/main/data/latest/historical_comparison.json)：2023、2024、2025 对应日期及前后 3 天的历史再分析对比。

## 预报窗口和解释

当前日期距离目标日较远时，模型不会伪造一个“精确天气”。`target_summary.json` 会明确标记：

- `HRES_AVAILABLE`：ECMWF HRES 已覆盖，作为近期期预报参考；
- `GFS_AVAILABLE`：GFS 已覆盖；
- `ECMWF_ENSEMBLE_AVAILABLE`：只有 ECMWF 集合进入窗口；
- `GEFS_LONG_RANGE_AVAILABLE`：只进入 GEFS 0.5°远期范围，只看温度/降水/云量趋势和集合分歧；
- `NOT_YET_AVAILABLE`：尚未进入当前模型窗口，保持空值。

按 2026-09-22 的时间点，10/24—25 会先由 GEFS 远期趋势覆盖；10/31—11/1 通常要到约 9/26—27 才进入 35 日 GEFS 范围，约 10/17—18 才进入 HRES/GFS 近中期范围。模型每天更新，覆盖状态会自动前移。

近期期重点看 `temperature_min_c`、`temperature_max_c`、`night_min_c`、`precipitation_mm`、`snowfall_cm`、`cloud_cover_mean_pct` 和 `cloud_cover_low_mean_pct`。集合模块另外给出中位数、P10/P90、降水/降雪/低温/高云量概率。

降水是区间累计量，逐日统计只把接口返回的每个区间值相加一次。集合和 GEFS 请求使用 `hourly_3` 原生粒度，避免对模型原生 3 小时输出做额外的步长猜测；标准 Forecast API 的逐小时降水则按接口返回的逐小时区间值累计。`precipitation_gt_0_5mm` 只表示当天有可测降水，判断是否值得担心应同时看 `precipitation_gt_2mm`、`precipitation_gt_5mm`、累计量和集合分歧。

35 日 GEFS 的“有降水概率”容易表现为整个可用月份的背景信号，不能解读成某一天已经锁定会下大雨。尤其在 30 天以上提前量，应优先看中位数、P10/P90、`precipitation_gt_5mm` 和不同模型后续收敛；高概率的 0.5 mm 阈值本身不等于影响行程的降雨。

历史对比按目标日期块前后各 3 天展开：四姑娘山为每年 10/21—10/28，九寨沟为每年 10/28—11/4，覆盖 2023—2025 三个完整年份。`historical_comparison.json` 使用 Open-Meteo Historical Weather API 的再分析格点记录已发生天气，用来回答“去年是否也这样”和“目标日前后是否只是单日异常”；它不是景区实测站，也不能替代当前年份的临近预报。

历史逐日记录另外按当地时间拆成 `00:00—06:00`、`06:00—12:00`、`12:00—18:00`、`18:00—24:00` 四段，分别统计总降水、液态雨、降雪、降水小时数和云量。分段依据接口返回的本地时间标签；Open-Meteo 的小时降水是此前一小时的累计值，因此边界时刻的降水归入其返回时间所在的时段，适合判断“偏夜间/上午/下午”的历史倾向，不应当解释成分钟级起止时间。

## 固定点位

坐标注册表见 [`config/points.json`](config/points.json)。双桥沟和毕棚沟使用游客中心代表点；理小路使用高位垭口代表点；九寨沟拆成树正寨、诺日朗、原始森林和长海，避免用沟口一个点替代整条沟的海拔梯度。

## 自动更新

`.github/workflows/update-weather.yml` 在北京时间 02:17、08:17、14:17、20:17 运行，也支持手动触发。每轮会运行单元测试、抓取 Open-Meteo、写入 `data/latest/`，并把结果归档到 `data/archive/YYYY-MM-DD/`。GEFS 原始成员数组压缩保存在当天归档的 `raw/gefs.json.gz`，最新入口保留可直接阅读的逐日分布。

本地运行：

```bash
python3 -m pip install -r requirements.txt
python3 -m unittest discover -s tests -v
python3 src/pipeline.py
```

资料来源：

- [Open-Meteo Forecast API](https://open-meteo.com/en/docs)
- [Open-Meteo Ensemble API](https://open-meteo.com/en/docs/ensemble-api)
- [Open-Meteo GFS 0.5°文档](https://open-meteo.com/en/docs/ensemble-api#gfs-ensemble-05)
- [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)

天气、景区开放、道路、景交和住宿仍需在临行前按当次证据复核。
