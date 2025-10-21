"""
Setup script for TABC Restaurant Data Scraper
"""

from setuptools import setup, find_packages
import os

# Read the contents of README file
this_directory = os.path.abspath(os.path.dirname(__file__))
try:
    with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = "TABC Restaurant Data Scraper - A comprehensive tool for collecting, enriching, and analyzing Texas restaurant data from the Comptroller API."

# Read requirements from requirements.txt
def read_requirements():
    """Read requirements from requirements.txt file"""
    requirements_path = os.path.join(this_directory, 'requirements.txt')
    try:
        with open(requirements_path, 'r') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    except FileNotFoundError:
        return []

setup(
    name='tabc-scrape',
    version='1.0.0',
    author='TABC Scraper Team',
    author_email='info@tabc-scrape.com',
    description='A comprehensive CLI tool for collecting, enriching, and analyzing Texas restaurant data from the Comptroller API',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/tabc-scrape/tabc-scrape',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Database',
        'Topic :: Internet :: WWW/HTTP :: Indexing/Search',
    ],
    python_requires='>=3.8',
    install_requires=read_requirements(),
    extras_require={
        'dev': [
            'pytest>=7.0.0',
            'pytest-asyncio>=0.21.0',
            'black>=22.0.0',
            'flake8>=5.0.0',
        ],
    },
    entry_points={
        'console_scripts': [
            'tabc-scrape=tabc_scrape.cli:cli',
        ],
    },
    include_package_data=True,
    zip_safe=False,
    keywords=[
        'texas', 'restaurant', 'data-scraping', 'api', 'comptroller',
        'data-enrichment', 'cli', 'database', 'validation'
    ],
)