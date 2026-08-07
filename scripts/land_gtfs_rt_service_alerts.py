from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import AlreadyExists

DEFAULT_SOURCE_URL = "https://bct.tmix.se/gtfs-realtime/alerts.pb?operatorIds=48"

DEFAULT_OUTPUT_ROOT = Path("output/service_alerts")

DEFAULT_VOLUME_DIRECTORY = (
    "/Volumes/bc_transit/dev/transit_landing/raw/rt_service_alerts"
)

ALLOWED_CONTENT_TYPES = {
    "application/x-protobuf",
    "application/octet-stream",
}


def write_manifest(
    output_root: Path,
    *,
    attempt_id: str,
    fetched_at: datetime,
    payload: dict,
) -> Path:
    """
    为每次 polling attempt 写入一份 JSON manifest。

    manifest 使用时间戳作为身份，因为它记录的是“抓取尝试”，
    而不是唯一的数据内容。即使两次抓取取得相同的 .pb，
    两次 attempt 仍然都应留下审计记录。
    """

    manifest_directory = (
        output_root / "manifests" / f"attempt_date={fetched_at:%Y-%m-%d}"
    )

    manifest_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    final_path = manifest_directory / f"{attempt_id}.json"

    # 先写入不被消费者读取的临时文件。
    staging_path = manifest_directory / f".{attempt_id}.json.part"

    staging_path.write_text(
        json.dumps(
            payload,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    # 完整写入后才发布最终 manifest。
    staging_path.replace(final_path)

    return final_path


def publish_protobuf(
    output_root: Path,
    *,
    body: bytes,
    attempt_id: str,
) -> tuple[str, Path, str]:
    """
    将 Protobuf payload 发布到 content-addressed outbox。

    返回：
    - content_sha256
    - published_path
    - publish_status

    最终路径中不能包含抓取时间。否则相同内容在不同时间运行时，
    会得到不同路径，并被 Auto Loader 当成不同物理文件。
    """

    content_sha256 = hashlib.sha256(body).hexdigest()

    payload_directory = output_root / "payloads"

    payload_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    # 使用完整 SHA-256，而不是只使用前 12 位。
    # 相同 bytes 在任何日期、任何小时都会映射到同一路径。
    file_name = f"service_alerts_{content_sha256}.pb"

    published_path = payload_directory / file_name

    if published_path.exists():
        # 相同内容已经发布过，不覆盖、不更新时间、不创建新路径。
        return (
            content_sha256,
            published_path,
            "UNCHANGED",
        )

    # 临时文件不以 .pb 结尾。
    # 将来上传到 Volume 时，Auto Loader 不会读取未完成文件。
    staging_path = payload_directory / f".{file_name}.{attempt_id}.part"

    staging_path.write_bytes(body)

    # 只有完整写入后才发布最终 .pb。
    staging_path.replace(published_path)

    return (
        content_sha256,
        published_path,
        "PUBLISHED",
    )


def publish_to_volume(
    local_path: Path,
    *,
    volume_directory: str,
    profile: str | None = None,
    workspace_client: Any | None = None,
) -> tuple[str, str]:
    """
    将本地 content-addressed .pb 发布到 UC Volume。

    返回：
    - volume_file_path
    - volume_publish_status（UPLOADED 或 ALREADY_EXISTS）

    目标文件名沿用本地 SHA-256 文件名，并且明确禁止覆盖：
    - 文件不存在：上传并返回 UPLOADED。
    - 文件已存在：说明相同内容已经发布，返回 ALREADY_EXISTS。

    workspace_client 参数只用于测试时注入 fake client；正常运行时
    使用 Databricks unified authentication 创建 WorkspaceClient。
    """

    normalized_directory = volume_directory.rstrip("/")

    if not normalized_directory.startswith("/Volumes/"):
        raise ValueError(
            "volume_directory must be an absolute UC Volume path under /Volumes/"
        )

    volume_file_path = f"{normalized_directory}/{local_path.name}"

    client = workspace_client

    if client is None:
        client = (
            WorkspaceClient(profile=profile)
            if profile
            else WorkspaceClient()
        )

    # create_directory 使用幂等的 PUT 语义；目录已存在时也会成功。
    client.files.create_directory(normalized_directory)

    try:
        # 不允许覆盖已经发布的 landing 文件。
        # Auto Loader 因此可以把每个路径视为不可变的 source file。
        client.files.upload_from(
            volume_file_path,
            str(local_path),
            overwrite=False,
        )

    except AlreadyExists:
        return (
            volume_file_path,
            "ALREADY_EXISTS",
        )

    return (
        volume_file_path,
        "UPLOADED",
    )


def fetch_protobuf(
    source_url: str,
) -> tuple[
    bytes,
    int,
    str,
    str | None,
    str | None,
]:
    """
    从 BC Transit 获取一次 GTFS-Realtime Protobuf 响应。

    Poller 本身只执行一次 HTTP 请求。
    循环和调度应由 GitHub Actions、Cloud Run、cron 等外部系统负责。
    """

    request = Request(
        source_url,
        headers={
            "User-Agent": (
                "bc-transit-gtfsrt-poller/0.1 (educational data engineering project)"
            ),
            "Accept": ("application/x-protobuf,application/octet-stream"),
        },
    )

    with urlopen(
        request,
        timeout=30,
    ) as response:
        body = response.read()
        status_code = response.status

        content_type = response.headers.get(
            "Content-Type",
            "unknown",
        )

        etag = response.headers.get("ETag")

        last_modified = response.headers.get("Last-Modified")

    return (
        body,
        status_code,
        content_type,
        etag,
        last_modified,
    )


def parse_arguments() -> argparse.Namespace:
    """
    读取命令行参数。

    默认值适合本地开发；将来部署时可以由 CI/CD 或 scheduler 覆盖。
    """

    parser = argparse.ArgumentParser(
        description=("Poll BC Transit GTFS-Realtime Service Alerts.")
    )

    parser.add_argument(
        "--source-url",
        default=DEFAULT_SOURCE_URL,
        help=("BC Transit GTFS-RT Service Alerts endpoint"),
    )

    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help=(
            "Local outbox root. Unique .pb files "
            "are written under payloads/ and "
            "attempt manifests under manifests/."
        ),
    )

    parser.add_argument(
        "--volume-directory",
        default=None,
        help=(
            "Optional UC Volume directory. When provided, the unique .pb file "
            "is uploaded after it is written to the local outbox. "
            f"Development value: {DEFAULT_VOLUME_DIRECTORY}"
        ),
    )

    parser.add_argument(
        "--databricks-profile",
        default=None,
        help=(
            "Optional Databricks configuration profile. "
            "If omitted, unified authentication uses its normal defaults."
        ),
    )

    return parser.parse_args()


def main() -> int:
    """
    执行一次完整 polling attempt。

    默认只输出到本地 outbox。传入 --volume-directory 后，
    同一次运行还会把 content-addressed .pb 发布到 UC Volume。
    """

    args = parse_arguments()

    fetched_at = datetime.now(UTC)

    attempt_id = fetched_at.strftime("%Y%m%dT%H%M%S.%fZ")

    base_manifest = {
        "attempt_id": attempt_id,
        "fetched_at_utc": (fetched_at.isoformat()),
        "source_url": args.source_url,
    }

    try:
        (
            body,
            status_code,
            content_type,
            etag,
            last_modified,
        ) = fetch_protobuf(args.source_url)

        if status_code != 200:
            raise RuntimeError(f"Unexpected HTTP status: {status_code}")

        if not body:
            raise RuntimeError("BC Transit returned an empty response")

        # Content-Type 可能带有 charset 等参数，因此只比较主类型。
        normalized_content_type = content_type.split(";", maxsplit=1)[0].strip().lower()

        if normalized_content_type not in ALLOWED_CONTENT_TYPES:
            raise RuntimeError(f"Unexpected Content-Type: {content_type}")

        (
            content_sha256,
            published_path,
            outbox_publish_status,
        ) = publish_protobuf(
            args.output_root,
            body=body,
            attempt_id=attempt_id,
        )

        volume_file_path = None
        volume_publish_status = "NOT_REQUESTED"

        if args.volume_directory:
            # 即使本地文件已经存在，也再次尝试远端发布。
            # 这样可以修复“本地写入成功、上一次远端上传失败”的情况。
            (
                volume_file_path,
                volume_publish_status,
            ) = publish_to_volume(
                published_path,
                volume_directory=args.volume_directory,
                profile=args.databricks_profile,
            )

        success_manifest = {
            **base_manifest,
            "poll_status": "SUCCESS",
            "outbox_publish_status": outbox_publish_status,
            "volume_publish_status": volume_publish_status,
            "http_status": status_code,
            "content_type": content_type,
            "byte_size": len(body),
            "content_sha256": content_sha256,
            "published_path": str(published_path),
            "volume_file_path": volume_file_path,
            "etag": etag,
            "last_modified": last_modified,
        }

        manifest_path = write_manifest(
            args.output_root,
            attempt_id=attempt_id,
            fetched_at=fetched_at,
            payload=success_manifest,
        )

        result = {
            **success_manifest,
            "manifest_path": str(manifest_path),
        }

        # 输出结构化 JSON，方便未来 scheduler 收集运行结果。
        print(
            json.dumps(
                result,
                indent=2,
                sort_keys=True,
            )
        )

        return 0

    except HTTPError as error:
        failure_manifest = {
            **base_manifest,
            "poll_status": "FAILED",
            "outbox_publish_status": "NOT_PUBLISHED",
            "volume_publish_status": "NOT_ATTEMPTED",
            "http_status": error.code,
            "error_type": "HTTP_ERROR",
            "error_message": str(error),
        }

    except URLError as error:
        failure_manifest = {
            **base_manifest,
            "poll_status": "FAILED",
            "outbox_publish_status": "NOT_PUBLISHED",
            "volume_publish_status": "NOT_ATTEMPTED",
            "error_type": "URL_ERROR",
            "error_message": str(error.reason),
        }

    # CLI 顶层需要把未知失败也写入审计 manifest 后再返回非零退出码。
    # 这里记录后不会忽略异常：failure manifest 会保留 error_type/message。
    except Exception as error:  # noqa: BLE001
        failure_manifest = {
            **base_manifest,
            "poll_status": "FAILED",
            "outbox_publish_status": "NOT_PUBLISHED",
            "volume_publish_status": "NOT_ATTEMPTED",
            "error_type": type(error).__name__,
            "error_message": str(error),
        }

    # 所有失败 attempt 也必须留下 manifest。
    manifest_path = write_manifest(
        args.output_root,
        attempt_id=attempt_id,
        fetched_at=fetched_at,
        payload=failure_manifest,
    )

    failure_result = {
        **failure_manifest,
        "manifest_path": str(manifest_path),
    }

    print(
        json.dumps(
            failure_result,
            indent=2,
            sort_keys=True,
        ),
        file=sys.stderr,
    )

    return 1


if __name__ == "__main__":
    sys.exit(main())
