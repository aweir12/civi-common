# setup.py

from setuptools import setup

setup(
    name="civi-common",
    version="0.0.1",
    py_modules=["civi-common"],  # since it's a single .py file
    install_requires=[
        "uuid",
        "datetime",
        "pyspark",
        "delta"
    ],
    author="Austin Weir",
    description="Common services to use with MSFT Fabric",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    python_requires=">=3.7",
)
