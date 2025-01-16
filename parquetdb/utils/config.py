import os
from pathlib import Path

from dotenv import load_dotenv
from variconfig import LoggingConfig

load_dotenv()

FILE = Path(__file__).resolve()
PKG_DIR = str(FILE.parents[1])
UTILS_DIR = str(FILE.parents[0])
DATA_DIR = os.getenv("DATA_DIR")


config = LoggingConfig.from_yaml(os.path.join(UTILS_DIR, "config.yml"))

if DATA_DIR:
    config.data_dir = DATA_DIR
