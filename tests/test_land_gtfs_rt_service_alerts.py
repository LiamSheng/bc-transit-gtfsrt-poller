from pathlib import Path

from databricks.sdk.errors.platform import AlreadyExists

from scripts.land_gtfs_rt_service_alerts import (
    publish_protobuf,
    publish_to_volume,
)


class FakeFilesApi:
    """记录测试期间准备发送给 Databricks Files API 的参数。"""

    def __init__(self) -> None:
        self.created_directories: list[str] = []
        self.uploads: list[tuple[str, str, bool]] = []

    def create_directory(self, path: str) -> None:
        self.created_directories.append(path)

    def upload_from(
        self,
        file_path: str,
        source_path: str,
        *,
        overwrite: bool,
    ) -> None:
        self.uploads.append((file_path, source_path, overwrite))


class FakeWorkspaceClient:
    def __init__(self, files: FakeFilesApi | None = None) -> None:
        self.files = files or FakeFilesApi()


class AlreadyExistingFilesApi(FakeFilesApi):
    """模拟 UC Volume 中目标 SHA 文件已经存在。"""

    def upload_from(
        self,
        file_path: str,
        source_path: str,
        *,
        overwrite: bool,
    ) -> None:
        raise AlreadyExists(
            "The file being created already exists.",
            error_code="ALREADY_EXISTS",
        )


def test_same_content_reuses_one_local_payload(tmp_path: Path) -> None:
    body = b"same-protobuf-content"

    first = publish_protobuf(
        tmp_path,
        body=body,
        attempt_id="attempt-001",
    )

    second = publish_protobuf(
        tmp_path,
        body=body,
        attempt_id="attempt-002",
    )

    assert first[0] == second[0]
    assert first[1] == second[1]
    assert first[2] == "PUBLISHED"
    assert second[2] == "UNCHANGED"
    assert len(list((tmp_path / "payloads").glob("*.pb"))) == 1
    assert list((tmp_path / "payloads").glob("*.part")) == []


def test_changed_content_creates_a_new_local_payload(tmp_path: Path) -> None:
    first = publish_protobuf(
        tmp_path,
        body=b"protobuf-version-1",
        attempt_id="attempt-001",
    )

    second = publish_protobuf(
        tmp_path,
        body=b"protobuf-version-2",
        attempt_id="attempt-002",
    )

    assert first[0] != second[0]
    assert first[1] != second[1]
    assert len(list((tmp_path / "payloads").glob("*.pb"))) == 2


def test_volume_upload_uses_sha_filename_without_overwrite(tmp_path: Path) -> None:
    local_path = tmp_path / ("service_alerts_" + "a" * 64 + ".pb")
    local_path.write_bytes(b"protobuf")

    client = FakeWorkspaceClient()

    remote_path, status = publish_to_volume(
        local_path,
        volume_directory=(
            "/Volumes/bc_transit/dev/transit_landing/raw/rt_service_alerts/"
        ),
        workspace_client=client,
    )

    assert remote_path == (
        "/Volumes/bc_transit/dev/transit_landing/raw/rt_service_alerts/"
        + local_path.name
    )
    assert status == "UPLOADED"
    assert client.files.created_directories == [
        "/Volumes/bc_transit/dev/transit_landing/raw/rt_service_alerts"
    ]
    assert client.files.uploads == [
        (
            remote_path,
            str(local_path),
            False,
        )
    ]


def test_volume_path_must_be_under_volumes(tmp_path: Path) -> None:
    local_path = tmp_path / "service_alerts.pb"
    local_path.write_bytes(b"protobuf")

    client = FakeWorkspaceClient()

    try:
        publish_to_volume(
            local_path,
            volume_directory="dbfs:/tmp/landing",
            workspace_client=client,
        )
    except ValueError as error:
        assert "/Volumes/" in str(error)
    else:
        raise AssertionError("Expected a ValueError for a non-Volume path")


def test_existing_volume_file_is_an_idempotent_success(tmp_path: Path) -> None:
    local_path = tmp_path / ("service_alerts_" + "b" * 64 + ".pb")
    local_path.write_bytes(b"same-protobuf")

    client = FakeWorkspaceClient(files=AlreadyExistingFilesApi())

    remote_path, status = publish_to_volume(
        local_path,
        volume_directory=(
            "/Volumes/bc_transit/dev/transit_landing/raw/rt_service_alerts"
        ),
        workspace_client=client,
    )

    assert remote_path.endswith(local_path.name)
    assert status == "ALREADY_EXISTS"
