#!/usr/bin/env python

from setuptools import setup, find_packages
import versioneer

requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-parquet',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Intake parquet plugin',
    url='https://github.com/ContinuumIO/intake-parquet',
    maintainer='Martin Durant',
    maintainer_email='martin.durant@utoronto.ca',
    license='BSD',
    packages=find_packages(exclude=['tests']),
    # package_data={'': ['*.pcap', '*.yml', '*.html']},
    entry_points={
        'intake.drivers': ['parquet = intake_parquet.source:ParquetSource']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.md').read(),
    zip_safe=False,
)
