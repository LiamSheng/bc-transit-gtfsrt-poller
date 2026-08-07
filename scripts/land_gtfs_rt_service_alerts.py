from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


SOURCE_URL = "https://bct.tmix.se/gtfs-realtime/alerts.pb?operatorIds=48"

OUTPUT_ROOT = Path("output/service_alerts")


def main() -> None:
    fetched_at = datetime.now(timezone.utc)

    request = Request(
        SOURCE_URL,
        headers={
            "User-Agent": (
                "bc-transit-gtfsrt-poller/0.1 (educational data engineering project)"
            ),
            "Accept": "application/x-protobuf",
        },
    )

    try:
        with urlopen(request, timeout=30) as response:
            payload = response.read()
            status_code = response.status
            content_type = response.headers.get(
                "Content-Type",
                "unknown",
            )
    except HTTPError as error:
        raise RuntimeError(f"BC Transit returned HTTP {error.code}") from error
    except URLError as error:
        raise RuntimeError(f"Could not reach BC Transit: {error.reason}") from error

    if not payload:
        raise RuntimeError("BC Transit returned an empty response")

    checksum = hashlib.sha256(payload).hexdigest()
    timestamp = fetched_at.strftime("%Y%m%dT%H%M%SZ")

    output_directory = (
        OUTPUT_ROOT
        / f"ingest_date={fetched_at:%Y-%m-%d}"
        / f"ingest_hour={fetched_at:%H}"
    )
    output_directory.mkdir(parents=True, exist_ok=True)

    output_path = output_directory / (f"service_alerts_{timestamp}_{checksum[:12]}.pb")

    temporary_path = output_path.with_name(f"{output_path.name}.tmp")

    temporary_path.write_bytes(payload)
    temporary_path.replace(output_path)

    result = {
        "status": "success",
        "source_url": SOURCE_URL,
        "http_status": status_code,
        "content_type": content_type,
        "fetched_at_utc": fetched_at.isoformat(),
        "bytes": len(payload),
        "sha256": checksum,
        "output_path": str(output_path),
    }

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
