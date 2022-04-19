import logging as lg


class Logger:
    def __init__(self, filename):
        lg.basicConfig(filename=filename + ".logs",
                       level=lg.INFO,
                       format="%(asctime)s %(levelname)s %(message)s")

    def info(self, msg):
        lg.info(msg)

    def warning(self, msg):
        lg.warning(msg)

    def error(self, msg):
        lg.error(msg)

    def critical(self, msg):
        lg.critical(msg)

