# 数据契约

该项目的机器可读入口在 `data/latest/`：

- `status.json`：本轮抓取状态、模块 QA 和目标日覆盖范围。
- `summary.json`：面向目标日的压缩摘要。
- `target_summary.json`：逐目标日 × 固定点的温度、降水、云量和集合预报证据。
- `hres.json`：ECMWF HRES 近 15 日逐小时/逐日数据。
- `gfs.json`：NOAA GFS 近 16 日逐小时/逐日数据。
- `ensemble.json`：ECMWF 51 成员集合及逐日分布。
- `gefs.json`：NOAA GEFS 0.25° 近程与 0.5° 远期集合分布；长程原始成员数组写入按日归档的 gzip 文件。
- `historical_comparison.json`：2023—2025 对应日期及前后 3 天的 Open-Meteo 历史再分析，含逐日温度、降水、降水小时、雨雪和可用云量；每个日记录还按当地时间拆分为 00—06、06—12、12—18、18—24 四个时段，窗口统计中提供各时段总量和占比。

预报请求固定 `Asia/Shanghai`、`cell_selection=nearest`、`elevation=nan`；历史请求使用对应数量的 `elevation=nan` 并固定 `Asia/Shanghai`、`cell_selection=nearest`。所有请求记录请求坐标、返回格点、返回海拔、时区、检索时间和数据来源。代表点不是景区内每一处的实况站，目标日未进入模型窗口时必须保持 `NOT_YET_AVAILABLE`。
