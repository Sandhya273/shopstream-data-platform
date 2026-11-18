import gzip
import json

from ingestion.common.storage import LocalStorage
from ingestion.lambda_clickstream.handler import process_object, validate_line

GOOD = {"event_id": "e1", "event_type": "page_view", "event_ts": "2026-10-01T10:00:00.000Z", "session_id": "s1"}


def test_valid_event_passes():
    ev, err = validate_line(json.dumps(GOOD))
    assert err is None and ev["event_id"] == "e1"


def test_malformed_json_is_rejected():
    assert validate_line('{"event_id": "e1", "event_ty')[1] == "malformed_json"


def test_missing_fields_and_unknown_type():
    assert validate_line(json.dumps({**GOOD, "session_id": None}))[1] == "missing_fields:session_id"
    assert validate_line(json.dumps({**GOOD, "event_type": "hover"}))[1] == "unknown_event_type:hover"


def test_purchase_requires_order_id():
    assert validate_line(json.dumps({**GOOD, "event_type": "purchase", "properties": {}}))[1] == "purchase_without_order_id"


def test_process_object_splits_good_bad_and_dedupes(tmp_path):
    st = LocalStorage(tmp_path)
    lines = [json.dumps(GOOD), json.dumps(GOOD), "{broken", json.dumps({**GOOD, "event_id": "e2"})]
    st.write_bytes("clickstream/dt=2026-10-01/f.json.gz", gzip.compress("\n".join(lines).encode()))

    audit = process_object(st, "clickstream/dt=2026-10-01/f.json.gz")

    assert (audit["valid"], audit["quarantined"], audit["duplicates_dropped"]) == (2, 1, 1)
    assert audit["lines_in"] == 4
    out = gzip.decompress(st.read_bytes("clickstream_validated/dt=2026-10-01/f.json.gz")).decode().split()
    assert {json.loads(x)["event_id"] for x in out} == {"e1", "e2"}
    assert st.exists("quarantine/clickstream/dt=2026-10-01/f.json.gz")
