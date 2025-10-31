import logging

from colorama import Fore, Style


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = ColorFormatter(
        "%(asctime)s - %(filename)s - %(levelname)s - %(message)s"
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


class ColorFormatter(logging.Formatter):
    def format(self, record):
        if record.levelno == logging.INFO:
            level_color = Fore.GREEN
        elif record.levelno == logging.WARNING:
            level_color = Fore.YELLOW
        elif record.levelno == logging.ERROR:
            level_color = Fore.RED
        else:
            level_color = ""

        format_str = f"{level_color}%(asctime)s - %(filename)s - %(levelname)s - %(message)s{Style.RESET_ALL}"
        formatter = logging.Formatter(format_str)
        return formatter.format(record)
