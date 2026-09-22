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

## 预报窗口和解释

当前日期距离目标日较远时，模型不会伪造一个“精确天气”。`target_summary.json` 会明确标记：

- `HRES_AVAILABLE`：ECMWF HRES 已覆盖，作为近期期预报参考；
- `GFS_AVAILABLE`：GFS 已覆盖；
- `ECMWF_ENSEMBLE_AVAILABLE`：只有 ECMWF 集合进入窗口；
- `GEFS_LONG_RANGE_AVAILABLE`：只进入 GEFS 0.5°远期范围，只看温度/降水/云量趋势和集合分歧；
- `NOT_YET_AVAILABLE`：尚未进入当前模型窗口，保持空值。

按 2026-09-22 的时间点，10/24—25 会先由 GEFS 远期趋势覆盖；10/31—11/1 通常要到约 9/26—27 才进入 35 日 GEFS 范围，约 10/17—18 才进入 HRES/GFS 近中期范围。模型每天更新，覆盖状态会自动前移。

近期期重点看 `temperature_min_c`、`temperature_max_c`、`night_min_c`、`precipitation_mm`、`snowfall_cm`、`cloud_cover_mean_pct` 和 `cloud_cover_low_mean_pct`。集合模块另外给出中位数、P10/P90、降水/降雪/低温/高云量概率。

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

天气、景区开放、道路、景交和住宿仍需在临行前按当次证据复核。
