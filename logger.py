import logging
import sys

LOGGING_LEVEL = logging.DEBUG

level = LOGGING_LEVEL
fmt = logging.Formatter(
    fmt="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",)

shell_handler = logging.StreamHandler(
    sys.stdout)
shell_handler.setLevel(level)
shell_handler.setFormatter(fmt)

logging.basicConfig(level=level,
                    handlers=[shell_handler])
