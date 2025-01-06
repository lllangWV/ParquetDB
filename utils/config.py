import os
from pathlib import Path
from variconfig import LoggingConfig

FILE = Path(__file__).resolve()
PKG_DIR = str(FILE.parents[1])
UTILS_DIR = str(FILE.parents[0])


config = LoggingConfig.from_yaml(os.path.join(UTILS_DIR, 'config.yml'))
