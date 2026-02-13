from loguru import logger
import sys

# Configure default logger
logger.remove()  # remove default handler
logger.add(sys.stdout, level="INFO", format="{time} | {level} | {message}")

def get_logger(name: str = "pipeline"):
    return logger.bind(module=name)
