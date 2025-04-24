from parquetdb._version import __version__
from parquetdb.core.parquetdb import LoadConfig, NormalizeConfig, ParquetDB
from parquetdb.utils.config import config
from parquetdb.utils.log_utils import setup_logging
from parquetdb.utils.matplotlib_utils import DEFAULT_COLOR_MAP, DEFAULT_COLORS

setup_logging()

__all__ = ["ParquetDB", "NormalizeConfig", "LoadConfig", "config", "plot_hist"]
