#!/usr/bin/env python

from setuptools import setup, find_packages


requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-fastparquet',
    version='0.0.1',
    description='Intake fastparquet plugin',
    url='https://github.com/ContinuumIO/intake-fastparquet',
    maintainer='Martin Durant',
    maintainer_email='martin.durant@utoronto.ca',
    license='BSD',
    packages=find_packages(),
    # package_data={'': ['*.pcap', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.md').read(),
    zip_safe=False,
)
