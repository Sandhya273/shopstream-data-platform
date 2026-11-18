import json

import pyarrow.parquet as pq
import pytest

from ingestion.api_extractor.payments_extractor import FatalHTTPError, PaymentsExtractor, TransientHTTPError, with_retries
from ingestion.common.storage import LocalStorage


class FakeTransport:
    """Two pages; fails transiently the first `fail_times` calls."""

    def __init__(self, fail_times=0):
        self.calls, self.fail_times = 0, fail_times
        self.pages = {
            None: {"data": [self._p("p1", "2026-10-01T10:00:00+05:30"), self._p("p2", "2026-10-01T11:00:00+05:30")],
                   "has_more": True, "next_cursor": "c2"},
            "c2": {"data": [self._p("p3", "2026-10-02T09:00:00+05:30")], "has_more": False, "next_cursor": None},
        }

    @staticmethod
    def _p(pid, ts):
        return {"payment_id": pid, "order_id": 1, "amount": 10.0, "gateway_ts": ts, "fees": {"gateway_fee": 0.18}}

    def get_page(self, cursor, updated_since):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise TransientHTTPError(503)
        body = json.loads(json.dumps(self.pages[cursor]))
        if updated_since:
            body["data"] = [r for r in body["data"] if r["gateway_ts"] > updated_since]
        return body


def test_full_extract_writes_parquet_manifest_and_watermark(tmp_path):
    st = LocalStorage(tmp_path)
    res = PaymentsExtractor(FakeTransport(), st, sleep=lambda s: None).run()
    assert res.records == 3 and res.pages == 2
    table = pq.read_table(tmp_path / res.files[0])
    assert table.num_rows == 3
    assert json.loads(table.column("raw")[0].as_py())["fees"]["gateway_fee"] == 0.18   # nested JSON preserved
    assert json.loads(st.read_bytes(PaymentsExtractor.STATE_KEY))["gateway_ts"] == "2026-10-02T09:00:00+05:30"


def test_incremental_run_only_fetches_new_records(tmp_path):
    st = LocalStorage(tmp_path)
    PaymentsExtractor(FakeTransport(), st, sleep=lambda s: None).run()
    second = PaymentsExtractor(FakeTransport(), st, sleep=lambda s: None).run()
    assert second.records == 0 and second.files == []


def test_transient_errors_are_retried(tmp_path):
    res = PaymentsExtractor(FakeTransport(fail_times=2), LocalStorage(tmp_path), sleep=lambda s: None).run()
    assert res.retries == 2 and res.records == 3


def test_retries_give_up_after_max_attempts():
    def always_fail():
        raise TransientHTTPError(429)
    with pytest.raises(TransientHTTPError):
        with_retries(always_fail, max_attempts=3, sleep=lambda s: None)


def test_fatal_errors_are_not_retried():
    calls = []

    def unauthorized():
        calls.append(1)
        raise FatalHTTPError("401")
    with pytest.raises(FatalHTTPError):
        with_retries(unauthorized, sleep=lambda s: None)
    assert len(calls) == 1
