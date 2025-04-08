from setuptools import setup, find_packages

setup(
    name="civi_common",
    version="0.1.0",
    packages=find_packages(),
    install_requires=["datetime"],
    author="Austin Weir",
    description="Common services for working with MSFT Fabric",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    classifiers=["Programming Language :: Python :: 3"],
    python_requires=">=3.7",
)
