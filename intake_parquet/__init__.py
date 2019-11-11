
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

import intake
from intake_parquet.source import ParquetSource
