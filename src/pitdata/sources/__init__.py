"""Every source the validated layer knows how to build.

Each source module declares its own SourceConfig next to its parser (D-026);
this is the one place that lists them. A raw folder whose name is not a key
here makes `pit build` fail (D-028).
"""

from pitdata.sources import fama_french
from pitdata.validated import SourceConfig

CONFIGS: dict[str, SourceConfig] = {
    fama_french.SOURCE: fama_french.VALIDATION,
}
