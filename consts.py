import logging
import sys


# from: https://stackoverflow.com/a/35804945
def add_logging_level(levelName, levelNum, methodName=None):
    if not methodName:
        methodName = levelName.lower()

    if hasattr(logging, levelName):
        raise AttributeError("{} already defined in logging module".format(levelName))
    if hasattr(logging, methodName):
        raise AttributeError("{} already defined in logging module".format(methodName))
    if hasattr(logging.getLoggerClass(), methodName):
        raise AttributeError("{} already defined in logger class".format(methodName))

    def logForLevel(self, message, *args, **kwargs):
        if self.isEnabledFor(levelNum):
            self._log(levelNum, message, args, **kwargs)

    def logToRoot(message, *args, **kwargs):
        logging.log(levelNum, message, *args, **kwargs)

    logging.addLevelName(levelNum, levelName)
    setattr(logging, levelName, levelNum)
    setattr(logging.getLoggerClass(), methodName, logForLevel)
    setattr(logging, methodName, logToRoot)


def setup_logger():
    logging.basicConfig(
        stream=sys.stderr, format="%(asctime)s - [%(name)s][%(levelname)s]: %(message)s"
    )
    log = logging.getLogger("gdrive")
    log.setLevel(logging.INFO)

    add_logging_level("TRACE", logging.DEBUG - 5)

    return log


logger = setup_logger()

FOLDER_TYPE = "application/vnd.google-apps.folder"
SHORTCUT_TYPE = "application/vnd.google-apps.shortcut"

SCOPES = ["https://www.googleapis.com/auth/drive"]

BATCH_SIZE = 100
MAXIMUM_BACKOFF = 60
BACKOFF_RESET_SECONDS = 60

IGNORE_LIST = [
    ".git",
    ".idea",
    ".vscode",
    "__pycache__",
    "venv",
    "node_modules",
    "dist",
    "build",
    "target",
    "out",
    "bin",
    "obj",
    "logs",
    "cache",
    "cmake-build-debug",
]
