import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

packages = find_packages(exclude=['examples','test'])

if 'uxs._data' not in packages:
    packages.append('uxs._data')

setup(
   name='uxs',
   version='0.1.0',
   description='A universal crypto exchange websocket',
   long_description=README,
   long_description_content_type='text/markdown',
   url='https://github.com/binares/uxs',
   author='binares',
   author_email='binares@protonmail.com',
   packages=packages,
   package_data={'uxs._data': ['*']},
   # 3.5.3 is required by aiphttp>=3.0, which in turn
   # is required by ccxt
   python_requires='>=3.5.3',
   install_requires=[
       'aiohttp>=3.0',
       'ccxt>=1.20.81',
       'python_dateutil>=2.1',
       'pandas>=0.21',
       'PyYAML>=3.10',
       'stockstats>=0.2',
       'fons @ git+https://github.com/binares/fons.git',
       'wsclient @ git+https://github.com/binares/wsclient.git',
   ],
)

