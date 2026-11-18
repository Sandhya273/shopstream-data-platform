"""
Storage abstraction so the same ingestion code runs against Amazon S3 in AWS
and against a local folder (which mirrors the bucket layout) on a laptop / in CI.

    storage = get_storage()                    # STORAGE_BACKEND=s3|local
    storage.write_bytes("raw/payments/...", b"...")
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator, Protocol


class Storage(Protocol):
    def read_bytes(self, key: str) -> bytes: ...
    def write_bytes(self, key: str, data: bytes) -> None: ...
    def list_keys(self, prefix: str) -> Iterator[str]: ...
    def exists(self, key: str) -> bool: ...


class LocalStorage:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def _p(self, key: str) -> Path:
        return self.root / key

    def read_bytes(self, key: str) -> bytes:
        return self._p(key).read_bytes()

    def write_bytes(self, key: str, data: bytes) -> None:
        p = self._p(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_bytes(data)
        tmp.replace(p)  # atomic rename: readers never see half-written files

    def list_keys(self, prefix: str) -> Iterator[str]:
        base = self._p(prefix)
        if not base.exists():
            return iter(())
        return (str(p.relative_to(self.root)).replace(os.sep, "/") for p in sorted(base.rglob("*")) if p.is_file() and not p.name.endswith(".tmp"))

    def exists(self, key: str) -> bool:
        return self._p(key).exists()


class S3Storage:
    def __init__(self, bucket: str, client=None):
        import boto3  # imported lazily so local runs don't need AWS libs configured

        self.bucket = bucket
        self.s3 = client or boto3.client("s3")

    def read_bytes(self, key: str) -> bytes:
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def write_bytes(self, key: str, data: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data, ServerSideEncryption="aws:kms")

    def list_keys(self, prefix: str) -> Iterator[str]:
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield obj["Key"]

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False


def get_storage(root: str | None = None) -> Storage:
    backend = os.getenv("STORAGE_BACKEND", "local")
    if backend == "s3":
        return S3Storage(os.environ["RAW_BUCKET"])
    return LocalStorage(root or os.getenv("LOCAL_LAKE_ROOT", "landing"))
