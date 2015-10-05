import os
import sys

try:
    from setuptools import setup
except:
    from distutils.core import setup

readme_note = '''\
.. note::

   For the latest source, issues and discussion, etc, please visit the
   `GitHub repository <https://github.com/samuell/sciluigi>`_\n\n
'''

with open('README.rst') as fobj:
    long_description = readme_note + fobj.read()

setup(
    name='sciluigi',
    version='0.9.2b3',
    description='Helper library for writing dynamic, flexible workflows in luigi',
    long_description=long_description,
    author='Samuel Lampa',
    author_email='samuel.lampa@farmbio.uu.se',
    url='https://github.com/samuell/sciluigi',
    license='MIT',
    keywords='workflows workflow pipeline luigi',
    packages=[
        'sciluigi',
    ],
    install_requires=[
        'luigi'
        ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2 :: Only',
        'Programming Language :: Python :: 2.7',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Topic :: Scientific/Engineering :: Chemistry',
    ],
)
