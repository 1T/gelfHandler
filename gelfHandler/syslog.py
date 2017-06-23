import six
from logging.handlers import SysLogHandler


def addSysLogLevelName(level, levelName):
    SysLogHandler.priority_names[levelName] = level


def getSysLogLevelName(level):
    if isinstance(level, six.string_types):
        result = SysLogHandler.priority_names[level.lower()]
        return result
    elif isinstance(level, six.integer_types):
        results = [k for (k, v) in SysLogHandler.priority_names.items() if v == level]
        results.sort(key=lambda x: len(x))
        return results[-1].upper()
    else:
        raise ValueError("unknown syslog level name %s %s" % (type(level), repr(level)))
