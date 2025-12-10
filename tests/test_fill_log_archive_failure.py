import asyncio
import os
import sys
from types import ModuleType

from src.fill_log import FillLog


def test_fill_log_rotation_on_upload_failure(tmp_path, monkeypatch):
    coin = "FAIL:USD"
    state_dir = str(tmp_path)
    # set env to trigger upload path
    monkeypatch.setenv("HL_ARCHIVE_S3_BUCKET", "my-bucket")

    class FakeS3Client:
        def upload_file(self, filename, bucket, key):
            raise RuntimeError("simulated upload failure")

    fake_boto = ModuleType("boto3")

    def fake_client(name):
        assert name == "s3"
        return FakeS3Client()

    fake_boto.client = fake_client
    monkeypatch.setitem(sys.modules, "boto3", fake_boto)

    # small max_bytes to force rotation on first append
    fl = FillLog(coin, state_dir, max_bytes=1)

    async def inner():
        await fl.append({"side": "buy", "px": 1.0, "sz": 1.0, "time": 1})
        await fl.append({"side": "sell", "px": 2.0, "sz": 0.5, "time": 2})

    asyncio.run(inner())

    # After failure, compressed file should still exist on disk (upload failed but compression attempted)
    files = list(tmp_path.rglob("*.gz"))
    assert len(files) >= 1
