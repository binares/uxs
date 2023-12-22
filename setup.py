import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

with open(HERE / "requirements.txt", encoding="utf-8") as f:
    requirements = f.read().split("\n")

packages = find_packages(exclude=["examples", "gen", "test"])

if "uxs._data" not in packages:
    packages.append("uxs._data")

setup(
    name="uxs",
    version="0.6.1",
    description="A unified crypto exchange websocket",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/binares/uxs",
    author="binares",
    author_email="binares@protonmail.com",
    packages=packages,
    package_data={"uxs._data": ["*"]},
    # decided to drop 3.5 and 3.6 support as in reality ccxt doesn't seem to support those versions
    python_requires=">=3.7",
    install_requires=requirements,
)
