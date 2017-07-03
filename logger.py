import logging
import os
import sys


class LoggerException(Exception):
    pass


class Logger(object):

    def __init__(self, loggingParameters, loggerLevel=logging.DEBUG):
        self.log = logging.Logger("logger")
        self.log.setLevel(loggerLevel)
        handler = logging.StreamHandler(sys.stderr)
        self.log.addHandler(handler)
        self.toLog = list(map(lambda x: x.upper(), [yes for yes in loggingParameters if (
            not yes.endswith("filename") and loggingParameters.getboolean(yes))]))
        self.loggingParameters = loggingParameters
        self.logFiles = {}
        try:
            self.debug("Making ./data...")
            os.mkdir("./data")
        except OSError:
            pass
        for tl in self.toLog:
            if tl:
                self.logFiles[tl] = open(
                    loggingParameters[tl + "_FILENAME"], 'w+')

    def __getitem__(self, toLog):
        if toLog.startswith("LOG"):
            return toLog in self.toLog
        return False

    def checkFileEmpty(self, log):
        if log not in self.logFiles:
            raise LoggerException(log + " filename not in Logger")
        return os.stat(self.loggingParameters[log + "_FILENAME"]).st_size == 0

    def writeToFile(self, log, string, end='\r\n'):
        if log not in self.logFiles:
            raise LoggerException(log + " filename not in Logger")
        self.logFiles[log].write(string + end)
        self.logFiles[log].flush()

    def getLogger(self):
        return self.log

    def debug(self, string):
        self.log.debug(string)

    def info(self, string):
        self.log.info(string)

    def closeAll(self):
        for f in self.logFiles:
            f.close()

if __name__ == "__main__":
    import configparser
    config = configparser.ConfigParser()
    config.read("./config.ini")
    LOG = Logger(config["LOGGING"])
    print(LOG["LOG_AMOUNT"])
    print(LOG.checkFileEmpty("LOG_AMOUNT"))
    LOG.writeToFile("LOG_AMOUNT", "APPLE")
