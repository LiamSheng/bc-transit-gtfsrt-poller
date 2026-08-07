# BC Transit GTFS-Realtime Poller

这个仓库是 BC Transit 数据链路的生产端。它负责：

1. 获取 Service Alerts Protobuf；
2. 以完整 SHA-256 生成不可变文件名；
3. 保存本地 polling manifest；
4. 可选地把唯一 `.pb` 上传到 Databricks Unity Catalog Volume。

它不负责解析 Protobuf，也不运行 Lakeflow Pipeline。解析与下游表由
`demo_transit_telemetry` DAB 仓库负责。

## 本地环境

```bash
uv sync
```

## 只运行本地 poller

```bash
uv run python scripts/land_gtfs_rt_service_alerts.py
```

输出位置：

```text
output/service_alerts/payloads/service_alerts_<完整 SHA-256>.pb
output/service_alerts/manifests/attempt_date=YYYY-MM-DD/<attempt_id>.json
```

## 发布到开发环境 UC Volume

首先确认 Databricks CLI 的 `DEFAULT` profile 有效：

```bash
databricks auth login \
  --host https://8259556718515952.2.gcp.databricks.com \
  --profile DEFAULT
```

然后运行：

```bash
uv run python scripts/land_gtfs_rt_service_alerts.py \
  --volume-directory /Volumes/bc_transit/dev/transit_landing/raw/rt_service_alerts \
  --databricks-profile DEFAULT
```

上传使用 `overwrite=False`。相同内容始终映射到同一个远端文件：

```text
/Volumes/bc_transit/dev/transit_landing/raw/rt_service_alerts/
  service_alerts_<完整 SHA-256>.pb
```

## 与 DAB 仓库的契约

```text
poller
  -> UC Volume 中的不可变 .pb
  -> Auto Loader binaryFile Bronze
  -> from_protobuf Silver
  -> decode/structure failure quarantine
```

poller 不调用 DAB 源码，DAB 也不调用 poller 源码。两者只通过 UC Volume
路径、文件扩展名和内容寻址命名约定协作。

## 测试

```bash
uv run pytest
uv run ruff check .
```
