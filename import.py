import os, sys

def import_module(path, mname):
    version = sys.version
    fname = os.path.join(path, mname + '.py')
    if version[0:3] == '2.7':
        import imp
        m = imp.load_source(name, fname)
        return m
    elif version[0:3] in ['3.3', '3.4']:
        import importlib.util
        from importlib.machinery import SourceFileLoader
        m = SourceFileLoader(mname, fname).load_module()
        return m
    if version[0:3] in ['3.5', '3.6']:
        import importlib.util
        spec = importlib.util.spec_from_file_location(mname, fname)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        return m
    else:
        raise ValueError('Unsupported Python version')
