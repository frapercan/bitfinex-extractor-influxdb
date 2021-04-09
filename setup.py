import io
import os
import re

from setuptools import find_packages
from setuptools import setup



def read(filename):
    filename = os.path.join(os.path.dirname(__file__), filename)
    text_type = type(u"")
    with io.open(filename, mode="r", encoding='utf-8') as fd:
        return re.sub(text_type(r':[a-z]+:`~?(.*?)`'), text_type(r'``\1``'), fd.read())


setup(
    name="bitfinex_extractor_influxdb",
    version="0.6.0",
    url="https://github.com/frapercan/bitfinex-extractor-influxdb.git",
    license='MIT',

    author="frapercan",
    author_email="frapercan1@alum.us.es",

    description="Python bitfinex scrapping functions. Storing results into InfluxDB",
    long_description_content_type="text/x-rst",
    long_description=read("README.rst"),

    packages=find_packages(exclude=('tests',)),

    install_requires=[],

    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.8',
    ],
)
