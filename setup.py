# setup.py

import setuptools
from setuptools import setup, find_packages

if __name__ == "__main__":
    setup(
        name='jaf',  # Ensure consistency with pyproject.toml
        version='0.1.0',  # Ensure consistency with pyproject.toml
        packages=find_packages(include=["jaf", "jaf.*"]),  # Restrict to jaf and its subpackages
        include_package_data=True,  # Include package data as specified in MANIFEST.in or pyproject.toml
        entry_points={
            "console_scripts": [
                "jaffy=jaf.console_script:main"
            ]
        },
    )
