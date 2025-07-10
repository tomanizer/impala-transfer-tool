#!/usr/bin/env python3
"""
Setup script for the Impala Transfer Tool package.
"""

from setuptools import setup, find_packages
import os

# Read the README file
def read_readme():
    readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return "Database Data Transfer Tool"

# Read requirements
def read_requirements():
    requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    if os.path.exists(requirements_path):
        with open(requirements_path, 'r') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return []

setup(
    name="impala-transfer",
    version="2.0.0",
    author="Data Transfer Team",
    author_email="team@example.com",
    description="A tool for transferring large datasets from database clusters to HDFS/Hive",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/example/impala-transfer",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: System :: Distributed Computing",
    ],
    python_requires=">=3.7",
    install_requires=[
        "pandas>=1.3.0",
        "pyarrow>=5.0.0",
    ],
    extras_require={
        "impyla": ["impyla>=0.17.0"],
        "pyodbc": ["pyodbc>=4.0.0"],
        "sqlalchemy": ["sqlalchemy>=1.4.0"],
        "postgresql": ["sqlalchemy[postgresql]>=1.4.0"],
        "mysql": ["sqlalchemy[mysql]>=1.4.0"],
        "oracle": ["sqlalchemy[oracle]>=1.4.0"],
        "all": [
            "impyla>=0.17.0",
            "pyodbc>=4.0.0", 
            "sqlalchemy>=1.4.0",
            "psycopg2-binary>=2.9.0",
            "pymysql>=1.0.0",
            "cx-oracle>=8.0.0",
        ],
        "dev": [
            "pytest>=6.0.0",
            "pytest-cov>=2.0.0",
            "black>=21.0.0",
            "flake8>=3.8.0",
            "mypy>=0.800",
        ],
    },
    entry_points={
        "console_scripts": [
            "impala-transfer=impala_transfer.cli:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
    keywords="database transfer impala hdfs hive sqlalchemy pyodbc",
    project_urls={
        "Bug Reports": "https://github.com/example/impala-transfer/issues",
        "Source": "https://github.com/example/impala-transfer",
        "Documentation": "https://github.com/example/impala-transfer/blob/main/README.md",
    },
) 