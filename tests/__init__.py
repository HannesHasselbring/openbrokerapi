import base64

try:
    from gevent import monkey

    # monkey.patch_all()
except ImportError:
    # fine if no gevent is available
    pass

AUTH_HEADER = "Basic " + base64.b64encode(b":").decode("ascii")