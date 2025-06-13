#!/usr/bin/env python

import glob
import os
from os import path
from setuptools import setup, Extension
import sys
import importlib.util

# allow to build with cython, but disable by default.
# build by doing CYTHON_ENABLED=1 python setup.py build_ext --inplace
CYTHON_ENABLED = True if os.getenv('CYTHON_ENABLED', False) else False

MYDIR = path.abspath(os.path.dirname(__file__))
long_description = open(os.path.join(MYDIR, 'README.rst')).read()

# Load version using importlib
spec = importlib.util.spec_from_file_location(
    'version', path.join('.', 'hbom', 'version.py'))
version_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(version_module)
version = version_module.__version__

JYTHON = 'java' in sys.platform

try:
    sys.pypy_version_info
    PYPY = True
except AttributeError:
    PYPY = False

if PYPY or JYTHON:
    CYTHON = False
else:
    try:
        from Cython.Distutils import build_ext
        CYTHON = True
    except ImportError:
        print('\nNOTE: Cython not installed. '
              'hbom will still work fine, but may run '
              'a bit slower.\n')
        CYTHON = False

if CYTHON and CYTHON_ENABLED:
    def list_modules(dirname):
        filenames = glob.glob(path.join(dirname, '*.py'))

        module_names = []
        for name in filenames:
            module, ext = path.splitext(path.basename(name))
            if module != '__init__':
                module_names.append(module)

        return module_names

    ext_modules = [
        Extension('hbom.' + ext, [path.join('hbom', ext + '.py')])
        for ext in list_modules(path.join(MYDIR, 'hbom'))]

    cmdclass = {'build_ext': build_ext}

else:
    cmdclass = {}
    ext_modules = []


setup(
    name='hbom',
    version=version,
    description='Happy Bits Object Model',
    author='John Loehrer',
    author_email='john@happybits.co',
    url='https://github.com/happybits/hbom',
    packages=['hbom'],
    python_requires='>=3.9',
    install_requires=['redpipe>=4.2.0', 'future'],
    tests_require=[
        'mock',
        'tox',
        'testfixtures',
        'python-coveralls',
        'line_profiler',
        'python-coveralls',
        'Cython>=0.23.4',
        'redislite>=6.2.912183',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    license='',
    include_package_data=True,
    long_description=long_description,
    cmdclass=cmdclass,
    ext_modules=ext_modules
)
