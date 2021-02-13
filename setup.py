# Databricks notebook source
from setuptools import setup, find_packages

setup(
    name='tasks',
    version='0.1.0',
    license='proprietary',
    description='Module Experiment',

    author='taka-yayoi',
    author_email='takaaki.yayoi@databricks.com',
    url='databricks.com',

    packages=find_packages(where='src'),
    package_dir={'': 'src'},
)