
try:
    import redis
except ImportError:
    pass
else:
    from .types import *

__version__ = "0.1.0"
