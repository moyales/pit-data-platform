"""Every source the validated layer knows how to build.

Each source module declares its own configs next to its parser (D-026): a
SourceConfig that determines the records, and a QualityConfig for the report
(D-034). This is the one place that lists them. A raw folder whose name is not
a key here makes `pit build` fail (D-028).
"""

from pitdata.build import BuildConfig
from pitdata.sources import fama_french

CONFIGS: dict[str, BuildConfig] = {
    fama_french.SOURCE: BuildConfig(fama_french.VALIDATION, fama_french.QUALITY),
}
