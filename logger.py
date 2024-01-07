import logging
import sys

def conf_logger(level):
    out_level = logging.INFO
    if level == "debug":
        out_level = logging.DEBUG
    fmt = logging.Formatter(
        fmt="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",)

    shell_handler = logging.StreamHandler(
        sys.stdout)
    shell_handler.setLevel(out_level)
    shell_handler.setFormatter(fmt)

    logging.basicConfig(level=out_level,
                        handlers=[shell_handler])
