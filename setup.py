from setuptools import find_packages, setup

try:
    __version__ = open("VERSION", "r").read()
except FileNotFoundError:
    __version__ = "0.0.0"


setup(
    name="cage",
    version=__version__,
    description="The computational platform for Cage Therapeutics",
    url="",
    author="Dazhi Tan",
    author_email="dtan@cagetx.com",
    license="Private",
    packages=find_packages(),
    scripts=[],
    include_package_data=True,
    zip_safe=False,
)
