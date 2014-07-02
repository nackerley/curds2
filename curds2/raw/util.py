#
"""
Revert to old usage of the raw API for now...

Force new version to act like old version.
todo: force old version to act like new versions <-- harder
"""
class retcode_revert(object):
    """
    Decorator to fix shit.
    """
    def __init__(self, f):
        self.f = f
    
    def __call__(self, *args, **kwargs):
        out = self.f(*args, **kwargs)
        if isinstance(out, tuple) and len(out) == 2 and isinstance(out[0], int):
            return out[1]
        else:
            return out


def oldversion(module, decorator=retcode_revert):
    """
    Replace all functions in a module with decorated versions
    """
    for a in dir(module):
        att = getattr(module, a)
        if hasattr(att, '__call__'):
            setattr(module, a, decorator(att))

