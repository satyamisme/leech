import os
from ast import literal_eval
from logging import getLogger

LOGGER = getLogger(__name__)

class Config:
    def __init__(self):
        self._env_config = {
            "BOT_TOKEN": str,
            "TELEGRAM_API": int,
            "TELEGRAM_HASH": str,
            "OWNER_ID": int,
            "LEECH_SPLIT_SIZE": int,
            "RSS_CHAT": int,
            "PREFERRED_LANGUAGES": str,
            # Add all your config keys here
        }
        self._config_dict = {}
        self.load_config()

    def load_config(self):
        for key, expected_type in self._env_config.items():
            value = os.environ.get(key)
            if not value:
                LOGGER.warning(f"Missing environment variable: {key}")
                self._config_dict[key] = None
                continue
            try:
                self._config_dict[key] = self._parse(value, expected_type)
            except Exception as e:
                LOGGER.error(f"Error parsing {key}: {e}")
                self._config_dict[key] = None

    def _parse(self, value, expected_type):
        if expected_type is bool:
            return str(value).lower() in ["true", "1", "y", "yes"]
        elif expected_type in [int, float]:
            return expected_type(value)
        elif expected_type is list or expected_type is tuple:
            try:
                return list(literal_eval(value))
            except:
                return value.split()
        else:
            return expected_type(value)

    def get_config(self):
        return self._config_dict

# ✅ Export config_dict globally
config_dict = Config().get_config()
