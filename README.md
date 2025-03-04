# Sample Python Project

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![PyPI version](https://badge.fury.io/py/sample-python-project.svg)](https://badge.fury.io/py/sample-python-project)

A sample Python project to demonstrate release-please integration with GitHub Actions.

## Features

- Simple greeting functionality
- Basic calculation operations
- Demonstrates release-please integration

## Installation

```bash
pip install sample_python_project
```

## Usage

```python
from sample_python_project import greet, calculate

# Greet a user
greeting = greet("World")
print(greeting)  # Output: Hello, World! Welcome to sample_python_project v0.1.0

# Perform calculations
result1 = calculate(5, 3, "add")
print(result1)  # Output: 8

result2 = calculate(10, 2, "divide")
print(result2)  # Output: 5
```

## Development

### Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/sample_python_project.git
cd sample_python_project

# Install development dependencies
pip install -e ".[dev]"
```

### Testing

```bash
pytest
```

### Code Formatting

```bash
black .
isort .
```

### Type Checking

```bash
mypy sample_python_project
```

## Release Process

This project uses [release-please](https://github.com/googleapis/release-please) to automate the release process:

1. Commits to the main branch are analyzed for [conventional commit messages](https://www.conventionalcommits.org/)
2. A release PR is automatically created or updated based on these commits
3. When the release PR is merged, a new GitHub release is created with:
   - Version number automatically incremented based on commit types
   - Changelog automatically generated from commit messages
   - GitHub release created with release notes
   - Python package automatically published to PyPI

### Commit Message Guidelines

This project follows the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification for commit messages. Please adhere to the following format when writing commit messages:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

**Types:**

- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `style`: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
- `refactor`: A code change that neither fixes a bug nor adds a feature
- `perf`: A code change that improves performance
- `test`: Adding missing or correcting existing tests
- `build`: Changes that affect the build system or external dependencies
- `ci`: Changes to our CI configuration files and scripts
- `chore`: Other changes that don't modify src or test files
- `revert`: Reverts a previous commit

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
