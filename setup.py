import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

setup(
   name='uxs',
   version='0.1.0',
   description='A universal crypto exchange websocket',
   long_description=README,
   long_description_content_type='text/markdown',
   url='https://github.com/binares/uxs',
   author='binares',
   author_email='binares@protonmail.com',
   license='MIT',
   packages=find_packages(exclude=['test']),
   python_requires='>=3.5',
   install_requires=[
       'aiohttp>=3.0',
       'filelock>=3.0',
       'python_dateutil>=2.1',
       'PyYAML>=3.10, <5',
       'prj @ git+https://github.com/binares/prj.git',
       'fons @ git+https://github.com/binares/fons.git',
       'wsclient @ git+https://github.com/binares/wsclient.git',
       'fintls @ git+https://github.com/binares/fintls.git',
   ],
)
