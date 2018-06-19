# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

with open('cromcost/version.py') as f:
    exec(f.read())

setup(
    name='cromwell-cost',
    version=__version__,
    description='Estimate cost of cromwell workflows on Google Cloud Platform',
    long_description=readme,
    author='David E. Larson',
    author_email='delarson@wustl.edu',
    license=license,
    url='https://github.com/ernfrid/cromwell_cost',
    install_requires=[
        'click==6.7',
        'clint==0.5.1',
        'google-api-python-client==1.7.3',
        'python-dateutil==2.7.3',
        'requests==2.19.1'
    ],
    entry_points='''
        [console_scripts]
        cost-it=cromcost.cli:cli
    ''',
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
    packages=find_packages(exclude=('tests', 'docs')),
    include_package_data=True,
)
