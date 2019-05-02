import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="geneshot",
    version="0.0.2",
    author="Jonathan Golob",
    author_email="j-dev@golob.org",
    description="A gene-level metagenomics pipeline",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jgolob/geneshot",
    packages=setuptools.find_packages(),
    dependency_links=[
        'https://github.com/jgolob/sciluigi/tarball/containertask',
    ],
    install_requires=[
        'sciluigi==2.0.1'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        'console_scripts': ['geneshot=geneshot.geneshot:main']
    }
)
