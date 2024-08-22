#!/usr/bin/env python

from setuptools import setup
from pathlib import Path

# Read the contents of the README file:
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='geoserver-rest',
    version='1.0.0',
    packages=['geoserver_rest'],
    description='library to access geoserver restapi',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/rockychen-dpaw/geoserver-rest',
    author='Department of Biodiversity, Conservation and Attractions',
    author_email='rocky.chen@dbca.wa.gov.au',
    maintainer='Department of Biodiversity, Conservation and Attractions',
    maintainer_email='rocky.chen@dbca.wa.gov.au',
    license='Apache License, Version 2.0',
    zip_safe=False,
    keywords=['geoserver'],
    install_requires=[
        'requests=2.25.0',
        'pytz=2024.1',
        'Jinja2=3.1.4'
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.12',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
