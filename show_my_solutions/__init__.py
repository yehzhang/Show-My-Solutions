__title__ = "show_my_solutions"
__version__ = "0.1"
__license__ = "MIT"

import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('flask').setLevel(logging.WARNING)
logging.getLogger('werkzeug').setLevel(logging.WARNING)
