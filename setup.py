"""Setup script for the sample_python_project package."""

import os
from setuptools import setup, find_packages

# Read the version from version.py without importing the package
with open(os.path.join("sample_python_project", "version.py"), "r") as f:
    for line in f:
        if line.startswith("__version__"):
            # Remove quotes and trim whitespace
            version = line.split("=")[1].strip().strip('"').strip("'")
            break

# Read the long description from README.md
with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="sample_python_project",
    version=version,
    description="A sample Python project for release-please integration",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Your Name",
    author_email="your.email@example.com",
    url="https://github.com/yourusername/sample_python_project",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "black>=23.0.0",
            "isort>=5.0.0",
            "mypy>=1.0.0",
            "flake8>=6.0.0",
        ],
    },
    project_urls={
        "Bug Reports": (
            "https://github.com/yourusername/sample_python_project/issues"
        ),
        "Source": "https://github.com/yourusername/sample_python_project",
    },
)
