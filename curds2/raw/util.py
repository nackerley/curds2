#
"""
Backwards compatibility for the raw Antelope API
"""
#
# Devel -- use newest version, make old versions compatible
#
class backwards(object):
    """
    Decorator to make old version of antelope methods backwards compatible.

    NOTES
    =====
    Works for versions older than 5.4, makes raw C interfaces return tuple
    variables with a (retcode, value) pair.
    """
    def __init__(self, f):
        self.f = f
    
    def __call__(self, *args, **kwargs):
        out = self.f(*args, **kwargs)
        return (0, out)


def patch_oldversion(module, methods=('_dbopen', '_dbgetv')):
    """
    Replace given functions in a module with decorated versions
    """
    for methodname in methods:
        method = getattr(module, methodname)
        setattr(module, methodname, backwards(method))


#
# Production -- force new version to act like old tested version.
#
class retcode_revert(object):
    """
    Decorator to revert 5.4 return types to value instead of tuple.
    """
    def __init__(self, f):
        self.f = f
    
    def __call__(self, *args, **kwargs):
        out = self.f(*args, **kwargs)
        if isinstance(out, tuple) and len(out) == 2 and isinstance(out[0], int):
            return out[1]
        else:
            return out


def patch_newversion(module, decorator=retcode_revert):
    """
    Replace all functions in a module with decorated versions
    """
    for a in dir(module):
        att = getattr(module, a)
        if hasattr(att, '__call__'):
            setattr(module, a, decorator(att))

