'''
Created on Oct 17, 2011

@author: kojima37
'''


def logger():
    import logging, sys

    # create logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # create formatter
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    # file handling
    hdlr = logging.FileHandler('./app.log')
    hdlr.setFormatter(formatter)
    sdlr = logging.StreamHandler(sys.stdout)
    sdlr.setFormatter(formatter)

    logger.addHandler(hdlr)
    logger.addHandler(sdlr)

    return logger


logger = logger()