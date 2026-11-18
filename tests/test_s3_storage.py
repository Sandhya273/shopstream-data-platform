import boto3
from moto import mock_aws

from ingestion.common.storage import S3Storage


@mock_aws
def test_s3_storage_roundtrip():
    s3 = boto3.client("s3", region_name="ap-south-1")
    s3.create_bucket(Bucket="raw", CreateBucketConfiguration={"LocationConstraint": "ap-south-1"})
    st = S3Storage("raw", client=s3)
    st.write_bytes("clickstream/dt=2026-10-01/a.json.gz", b"x")
    st.write_bytes("clickstream/dt=2026-10-02/b.json.gz", b"y")
    assert list(st.list_keys("clickstream/")) == ["clickstream/dt=2026-10-01/a.json.gz", "clickstream/dt=2026-10-02/b.json.gz"]
    assert st.read_bytes("clickstream/dt=2026-10-02/b.json.gz") == b"y"
    assert st.exists("clickstream/dt=2026-10-01/a.json.gz") and not st.exists("nope")
