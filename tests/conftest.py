"""Hypothesis settings for every property test (D-032).

The default profile keeps `pytest` and CI fast. For a deeper search, run:

    HYPOTHESIS_PROFILE=thorough uv run pytest tests/test_properties.py

No deadline in either: each example runs several DuckDB queries, and their
time varies enough (the first one in a process is slower) to trip
Hypothesis's per-example timer.
"""

import os

from hypothesis import settings

settings.register_profile("pitdata", max_examples=200, deadline=None)
settings.register_profile("thorough", max_examples=5000, deadline=None)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "pitdata"))
