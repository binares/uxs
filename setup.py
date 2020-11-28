import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

packages = find_packages(exclude=['examples','gen','test'])

if 'uxs._data' not in packages:
    packages.append('uxs._data')

setup(
   name='uxs',
   version='0.5.0',
   description='A unified crypto exchange websocket',
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
       'aiohttp_socks>=0.2',
       'ccxt>=1.38.5',
       'python_dateutil>=2.1',
       'pandas>=0.21',
       'PyYAML>=3.10',
       'fons>=0.2.1',
       'wsclient>=0.3.0',
   ],
)

