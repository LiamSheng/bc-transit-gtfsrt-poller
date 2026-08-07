```markdown
# BC Transit GTFS-Realtime Poller

This repository serves as the producer / ingestion component for the BC Transit data pipeline. Its primary responsibilities include:

1. Fetching GTFS-RT Service Alerts Protobuf payloads.
2. Generating immutable filenames using full SHA-256 hashes.
3. Persisting local polling manifests.
4. Optionally uploading unique `.pb` payloads to a Databricks Unity Catalog (UC) Volume.

This repository does **not** handle Protobuf parsing or execute Lakeflow Pipelines. Payload decoding, structural validation, and downstream Delta table processing are handled separately by the `demo_transit_telemetry` DAB (Databricks Asset Bundle) repository.

## Local Environment Setup

Sync project dependencies using `uv`:

```bash
uv sync

```

## Running Locally

Run the poller script locally without remote upload:

```bash
uv run python scripts/land_gtfs_rt_service_alerts.py

```

### Output Locations

```text
output/service_alerts/payloads/service_alerts_<FULL_SHA256>.pb
output/service_alerts/manifests/attempt_date=YYYY-MM-DD/<attempt_id>.json

```

## Publishing to Dev UC Volume

First, verify that your Databricks CLI `DEFAULT` authentication profile is active:

```bash
databricks auth login \
  --host [https://8259556718515952.2.gcp.databricks.com](https://8259556718515952.2.gcp.databricks.com) \
  --profile DEFAULT

```

Then, run the landing script with the target UC Volume directory specified:

```bash
uv run python scripts/land_gtfs_rt_service_alerts.py \
  --volume-directory /Volumes/bc_transit/dev/transit_landing/raw/rt_service_alerts \
  --databricks-profile DEFAULT

```

Uploads strictly enforce `overwrite=False` for idempotency. Identical payloads always map to the exact same remote file path:

```text
/Volumes/bc_transit/dev/transit_landing/raw/rt_service_alerts/
  service_alerts_<FULL_SHA256>.pb

```

## Interface Contract with DAB Repository

```text
poller
  -> Immutable .pb in UC Volume
  -> Auto Loader (binaryFile) Bronze
  -> from_protobuf Silver
  -> Decode/Structure failure quarantine

```

The poller and DAB repositories are completely decoupled; neither invokes or references code from the other. Inter-repository coordination relies solely on the UC Volume directory path, file extension (`.pb`), and content-addressed naming conventions.

## Testing & Quality Checks

```bash
uv run pytest
uv run ruff check .

```
