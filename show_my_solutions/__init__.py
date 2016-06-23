import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())
logging.basicConfig(level=logging.DEBUG)

logging.getLogger('requests').setLevel(logging.WARNING)
