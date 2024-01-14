import pathlib
from setuptools import setup, find_packages, Extension
from Cython.Build import cythonize
import numpy as np

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

with open(HERE / "requirements.txt", encoding="utf-8") as f:
    requirements = f.read().split("\n")

packages = find_packages(exclude=["delisted", "examples", "gen", "test"])

if "uxs._data" not in packages:
    packages.append("uxs._data")

extensions = [
    Extension(
        "uxs.fintls.shapes_cython",  # the output location
        sources=["uxs/fintls/shapes_cython.pyx"],
        include_dirs=[np.get_include()],
        extra_compile_args=["-O3"],
        language="c++",
    )
]

setup(
    name="uxs",
    version="0.7.0",
    description="A unified crypto exchange websocket",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/binares/uxs",
    author="binares",
    author_email="binares@protonmail.com",
    packages=packages,
    package_data={"uxs._data": ["*"], "uxs.fintls": ["*.pyx"]},
    # decided to drop 3.5 and 3.6 support as in reality ccxt doesn't seem to support those versions
    python_requires=">=3.7",
    install_requires=requirements,
    extras_require={"dev": ["pytest"]},  # "pytest-asyncio", "pytest-cov", "codecov"
    # ext_modules=cythonize("uxs/fintls/shapes_cython.pyx", language_level="3"),
    ext_modules=cythonize(extensions, language_level="3"),
)
