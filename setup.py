# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

with open('cromulent/version.py') as f:
    exec(f.read())

setup(
    name='cromulent',
    version=__version__,
    description='Cromwell helper for workflows run on the Google Cloud Platform',
    long_description=readme,
    author='David E. Larson',
    author_email='delarson@wustl.edu',
    license=license,
    url='https://github.com/ernfrid/cromwell_cost',
    install_requires=[
        'click>=7',
        'clint==0.5.1',
        'google-api-python-client==1.7.3',
        'google-auth==1.5.1',
        'python-dateutil==2.7.3',
        'requests==2.20.0',
        'tabulate==0.8.2',
        'cytoolz==0.9.0.1',
        'toolz==0.9.0',
        'PyMySQL==0.9.3',
        'pyparsing==2.3.1',
        'pyhocon==0.3.51'
    ],
    entry_points='''
        [console_scripts]
        cromulent=cromulent.cli:cli
    ''',
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
    packages=find_packages(exclude=('tests', 'docs')),
    include_package_data=True,
)
