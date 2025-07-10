# Contributing to Impala Transfer Tool

Thank you for your interest in contributing to the Impala Transfer Tool! This document provides guidelines and information for contributors.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Code Style](#code-style)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)
- [Issue Reporting](#issue-reporting)
- [Code of Conduct](#code-of-conduct)

## Getting Started

### Prerequisites

- Python 3.12 or higher
- Git
- pip
- Virtual environment tool (venv, conda, etc.)

### Fork and Clone

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/impala-transfer-tool.git
   cd impala-transfer-tool
   ```

3. **Add upstream remote**:
   ```bash
   git remote add upstream https://github.com/tomanizer/impala-transfer-tool.git
   ```

## Development Setup

### Environment Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
pip install -e .[dev]

# Install pre-commit hooks
pip install pre-commit
pre-commit install
```

### Verify Setup

```bash
# Run tests to verify installation
pytest tests/ -v

# Check code style
black --check impala_transfer/ tests/
flake8 impala_transfer/ tests/
```

## Code Style

### Python Style Guide

We follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) with these project-specific guidelines:

- **Line Length**: 88 characters (black default)
- **Docstrings**: Use reStructuredText format
- **Type Hints**: Use type hints for all function parameters and return values
- **Imports**: Group imports (standard library, third-party, local)

### Code Formatting

We use automated tools for code formatting:

```bash
# Format code with black
black impala_transfer/ tests/

# Sort imports with isort
isort impala_transfer/ tests/

# Check code style with flake8
flake8 impala_transfer/ tests/
```

### Pre-commit Hooks

Pre-commit hooks automatically run on each commit:

- **black**: Code formatting
- **isort**: Import sorting
- **flake8**: Code linting
- **mypy**: Type checking

### Docstring Format

Use reStructuredText format for docstrings:

```python
def transfer_data(source: str, target: str) -> bool:
    """Transfer data from source to target.
    
    :param source: Source location
    :type source: str
    :param target: Target location
    :type target: str
    :return: True if transfer successful
    :rtype: bool
    :raises ValueError: If source or target is invalid
    """
    pass
```

## Testing

### Running Tests

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest --cov=impala_transfer tests/

# Run specific test file
pytest tests/test_connection.py -v

# Run tests matching pattern
pytest -k "test_connect" -v

# Run tests with parallel execution
pytest -n auto tests/
```

### Writing Tests

#### Test Structure

Follow the modular test structure:

```python
#!/usr/bin/env python3
"""
Test suite for the connection module.
"""

import unittest
from unittest.mock import Mock, patch

from impala_transfer.connection import ConnectionManager


class TestConnectionManager(unittest.TestCase):
    """Test the ConnectionManager class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.connection_kwargs = {
            'source_host': 'test-host',
            'source_port': 21050,
            'source_database': 'test_db'
        }
    
    def test_connect_success(self):
        """Test successful connection."""
        # Test implementation
        pass
    
    def test_connect_failure(self):
        """Test connection failure."""
        # Test implementation
        pass
```

#### Test Guidelines

1. **Test File Naming**: Use `test_<module_name>.py` format
2. **Test Class Naming**: Use `Test<ClassName>` format
3. **Test Method Naming**: Use `test_<description>` format
4. **Mocking**: Mock external dependencies (database, file system)
5. **Coverage**: Aim for >90% line coverage
6. **Descriptive Names**: Use descriptive test names

#### Test Categories

- **Unit Tests**: Test individual functions/methods with mocks
- **Integration Tests**: Test component interactions
- **End-to-End Tests**: Test complete workflows

### Test Coverage

Maintain high test coverage:

```bash
# Generate coverage report
pytest --cov=impala_transfer --cov-report=html tests/

# View coverage report
open htmlcov/index.html
```

## Pull Request Process

### Before Submitting

1. **Update your fork**:
   ```bash
   git fetch upstream
   git checkout main
   git merge upstream/main
   ```

2. **Create feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make your changes**:
   - Follow code style guidelines
   - Add tests for new functionality
   - Update documentation

4. **Test thoroughly**:
   ```bash
   # Run all tests
   pytest tests/ -v
   
   # Check code style
   black --check impala_transfer/ tests/
   flake8 impala_transfer/ tests/
   
   # Check type hints
   mypy impala_transfer/
   ```

5. **Commit your changes**:
   ```bash
   git add .
   git commit -m "Add feature: brief description"
   ```

### Submitting Pull Request

1. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

2. **Create Pull Request**:
   - Go to your fork on GitHub
   - Click "New Pull Request"
   - Select your feature branch
   - Fill out the PR template

3. **PR Template**:
   ```markdown
   ## Description
   Brief description of changes

   ## Type of Change
   - [ ] Bug fix
   - [ ] New feature
   - [ ] Breaking change
   - [ ] Documentation update

   ## Testing
   - [ ] Tests added/updated
   - [ ] All tests passing
   - [ ] Manual testing completed

   ## Checklist
   - [ ] Code follows style guidelines
   - [ ] Self-review completed
   - [ ] Documentation updated
   - [ ] No breaking changes (or documented)
   ```

### Review Process

1. **Automated Checks**: CI/CD pipeline runs tests and style checks
2. **Code Review**: Maintainers review your code
3. **Feedback**: Address any feedback or requested changes
4. **Merge**: Once approved, your PR will be merged

## Issue Reporting

### Before Reporting

1. **Search existing issues** for similar problems
2. **Check documentation** for solutions
3. **Try troubleshooting** steps from the troubleshooting guide

### Creating Issues

Use the appropriate issue template:

#### Bug Report Template
```markdown
## Bug Description
Clear description of the bug

## Steps to Reproduce
1. Step 1
2. Step 2
3. Step 3

## Expected Behavior
What you expected to happen

## Actual Behavior
What actually happened

## Environment
- OS: [e.g., Ubuntu 20.04]
- Python Version: [e.g., 3.12.0]
- Impala Transfer Tool Version: [e.g., 2.0.0]
- Database Type: [e.g., Impyla, pyodbc, sqlalchemy]

## Additional Information
- Error messages/logs
- Configuration files
- Screenshots (if applicable)
```

#### Feature Request Template
```markdown
## Feature Description
Clear description of the feature

## Use Case
Why this feature is needed

## Proposed Solution
How you think it should work

## Alternatives Considered
Other approaches you considered

## Additional Information
- Related issues
- Implementation ideas
- Mockups (if applicable)
```

### Issue Guidelines

1. **Be specific**: Provide clear, detailed descriptions
2. **Include context**: Environment, configuration, steps to reproduce
3. **Attach logs**: Include relevant error messages and logs
4. **Use labels**: Help categorize and prioritize issues
5. **Follow up**: Respond to questions and provide additional information

## Code of Conduct

### Our Standards

We are committed to providing a welcoming and inspiring community for all. We expect all contributors to:

- **Be respectful** and inclusive
- **Be collaborative** and constructive
- **Be patient** and helpful
- **Be professional** and courteous

### Unacceptable Behavior

- Harassment or discrimination
- Trolling or insulting comments
- Publishing others' private information
- Other conduct inappropriate in a professional setting

### Enforcement

- Violations will be addressed by project maintainers
- Consequences may include warnings, temporary bans, or permanent bans
- Serious violations will be reported to appropriate authorities

## Development Workflow

### Branch Strategy

- **main**: Production-ready code
- **develop**: Integration branch for features
- **feature/***: Feature development branches
- **bugfix/***: Bug fix branches
- **hotfix/***: Critical bug fixes

### Commit Messages

Use conventional commit format:

```
type(scope): description

[optional body]

[optional footer]
```

Types:
- **feat**: New feature
- **fix**: Bug fix
- **docs**: Documentation changes
- **style**: Code style changes
- **refactor**: Code refactoring
- **test**: Test changes
- **chore**: Maintenance tasks

Examples:
```
feat(connection): add support for SSL connections

fix(cli): resolve argument parsing issue

docs(readme): update installation instructions
```

### Release Process

1. **Version bump**: Update version numbers
2. **Changelog**: Update CHANGELOG.md
3. **Testing**: Run full test suite
4. **Documentation**: Update documentation
5. **Tagging**: Create release tag
6. **Publishing**: Build and publish distribution

## Getting Help

### Resources

- **Documentation**: [README.md](README.md), [API_REFERENCE.md](API_REFERENCE.md)
- **Issues**: [GitHub Issues](https://github.com/tomanizer/impala-transfer-tool/issues)
- **Discussions**: [GitHub Discussions](https://github.com/tomanizer/impala-transfer-tool/discussions)
- **Wiki**: [Project Wiki](https://github.com/tomanizer/impala-transfer-tool/wiki)

### Communication

- **GitHub Issues**: For bug reports and feature requests
- **GitHub Discussions**: For questions and general discussion
- **Pull Requests**: For code contributions
- **Email**: For sensitive or private matters

### Mentorship

New contributors are welcome! We provide:

- **Code reviews** with constructive feedback
- **Documentation** to help you get started
- **Examples** and templates
- **Guidance** on best practices

## Recognition

Contributors are recognized in:

- **README.md**: List of contributors
- **CHANGELOG.md**: Credit for contributions
- **GitHub**: Contributor statistics and profile
- **Release notes**: Acknowledgment of contributions

Thank you for contributing to the Impala Transfer Tool! 🚀 