"""Test helpers shared by the validated-layer and property tests.

`make_ingest` builds one raw ingest as (meta.json contents, raw table), the
shape `derive_records` takes. `ingest_sequences` is the Hypothesis strategy
for random ingest histories (spec section 5).

pytest puts this folder on the import path (tests/ is not a package), so test
files import it as `strategies`.
"""

from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
from hypothesis import strategies as st

SOURCE = "ken_french"

# 2 series x 4 dates. The dates straddle month ends and a leap day, so lag-rule
# instants differ between keys.
SERIES = ("A.X", "A.Y")
DATES = (date(2024, 1, 31), date(2024, 2, 1), date(2024, 2, 29), date(2024, 3, 15))
KEYS = [(series, day) for series in SERIES for day in DATES]

# Ingests fall between FIRST_POSSIBLE_INGEST and about five months later: some
# before and some after the keys' rule instants, never before a valid date.
FIRST_POSSIBLE_INGEST = datetime(2024, 3, 16, tzinfo=UTC)
MAX_INGEST_HOURS = 150 * 24


def make_ingest(time: datetime, rows: dict, source: str = SOURCE) -> tuple[dict, pa.Table]:
    """One raw ingest. `rows` maps (series_id, valid_date) to a value (or None)."""
    ingest_id = f"{time:%Y%m%dT%H%M%SZ}_test"
    meta = {"ingest_id": ingest_id, "ingest_time": time.isoformat(), "source": source}
    keys = list(rows)
    table = pa.table(
        {
            "series_id": pa.array([k[0] for k in keys], type=pa.string()),
            "valid_date": pa.array([k[1] for k in keys], type=pa.date32()),
            "value": pa.array([rows[k] for k in keys], type=pa.float64()),
            "ingest_time": pa.array([time] * len(keys), type=pa.timestamp("us", tz="UTC")),
            "ingest_id": pa.array([ingest_id] * len(keys), type=pa.string()),
            "source": pa.array([source] * len(keys), type=pa.string()),
        }
    )
    return meta, table


def ingest_time(meta: dict) -> datetime:
    return datetime.fromisoformat(meta["ingest_time"])


@st.composite
def ingest_sequences(draw) -> list[tuple[dict, pa.Table]]:
    """1 to 6 ingests at distinct hourly times, in no particular order.

    Each ingest holds a random subset of KEYS (absent keys are common). Values
    come from a set of three plus null, so changes, reversions (A->B->A) and
    value<->null transitions all happen often.
    """
    offsets = draw(st.lists(st.integers(0, MAX_INGEST_HOURS), min_size=1, max_size=6, unique=True))
    ingests = []
    for hours in offsets:
        present = draw(st.lists(st.sampled_from(KEYS), unique=True, max_size=len(KEYS)))
        rows = {k: draw(st.sampled_from([0.25, -1.5, 3.0, None])) for k in present}
        ingests.append(make_ingest(FIRST_POSSIBLE_INGEST + timedelta(hours=hours), rows))
    return ingests
