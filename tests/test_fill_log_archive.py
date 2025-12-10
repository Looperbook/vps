import asyncio
import importlib
import os
import sys
from types import ModuleType

from src.fill_log import FillLog


def test_fill_log_rotates_and_uploads(tmp_path, monkeypatch):
    coin = "ARCH:USD"
    state_dir = str(tmp_path)
    # ensure env var triggers upload path
    monkeypatch.setenv("HL_ARCHIVE_S3_BUCKET", "my-bucket")

    uploads = []

    class FakeS3Client:
        def upload_file(self, filename, bucket, key):
            uploads.append((filename, bucket, key))

    fake_boto = ModuleType("boto3")

    def fake_client(name):
        assert name == "s3"
        return FakeS3Client()

    fake_boto.client = fake_client

    # inject fake boto3 into sys.modules so worker threads import it
    monkeypatch.setitem(sys.modules, "boto3", fake_boto)

    # small max_bytes to force rotation on first append
    fl = FillLog(coin, state_dir, max_bytes=1)

    async def inner():
        await fl.append({"side": "buy", "px": 1.0, "sz": 1.0, "time": 1})
        # second append should trigger rotation+compress+upload
        await fl.append({"side": "sell", "px": 2.0, "sz": 0.5, "time": 2})

    asyncio.run(inner())

    # Expect at least one upload attempt
    assert len(uploads) >= 1
    fname, bucket, key = uploads[0]
    assert bucket == "my-bucket"
    # uploaded file should end with .gz
    assert str(fname).endswith(".gz") or str(key).endswith('.gz')
