from setuptools import find_packages, setup

try:
    __version__ = open("VERSION", "r").read()
except FileNotFoundError:
    __version__ = "0.0.0"


setup(
    name="tickers",
    version=__version__,
    description="Tickers",
    url="",
    author="Dazhi Tan",
    author_email="dazhi.tan.ma@gmail.com",
    license="Private",
    packages=find_packages(),
    scripts=[],
    include_package_data=True,
    zip_safe=False,
)
