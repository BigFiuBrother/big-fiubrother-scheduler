from setuptools import setup, find_packages

with open("README.md", 'r') as f:
    long_description = f.read()

setup(
   name='big-fiubrother-scheduler',
   version='0.0.1',
   description='Big Fiubrother Scheduler application',
   license="GPLv3",
   long_description=long_description,
   long_description_content_type='text/markdown',
   author='Eduardo Neira, Gabriel Gayoso',
   author_email='aneira@fi.uba.ar',
   packages=find_packages(),
   url= 'https://github.com/BigFiuBrother/big-fiubrother-scheduler'
)