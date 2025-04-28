import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from platformdirs import user_config_dir
from variconfig import ConfigDict

logger = logging.getLogger(__name__)

load_dotenv()

FILE = Path(__file__).resolve()
PKG_DIR = str(FILE.parents[1])
UTILS_DIR = str(FILE.parents[0])
DATA_DIR = os.getenv("DATA_DIR")

DEFAULT_CFG = os.path.join(UTILS_DIR, "config.yml")


def load_config() -> ConfigDict:
    """
    Load and merge configuration files, highest-priority first.
    """
    user_cfg_dir = Path(user_config_dir("parquetdb"))
    user_cfg = user_cfg_dir / "config.yml"
    if not user_cfg.exists():
        user_cfg_dir.mkdir(parents=True, exist_ok=True)
        if Path(DEFAULT_CFG).exists() and not user_cfg.exists():
            import shutil

            shutil.copy2(DEFAULT_CFG, user_cfg)
            logger.info(f"Created user config file at {user_cfg}")

    user_cfg = Path(user_config_dir("parquetdb")) / "config.yml"

    logger.info(f"Config file: {user_cfg}")
    cfg = ConfigDict.from_yaml(str(user_cfg))

    if DATA_DIR:
        logger.info(f"Setting data_dir to {DATA_DIR}")
        cfg.data_dir = DATA_DIR

    return cfg


config = load_config()
