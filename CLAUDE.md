# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

HBOM (Happy Bits Object Model) is a Python library that provides a Redis-backed object model, extending OMA (Object Model Architecture). It uses redpipe for Redis pipeline operations and supports both Redis and Redis Cluster backends.

## Core Architecture

- **Definition**: Base metaclass system for defining models with field validation and primary key enforcement
- **Fields**: Field types for model attributes with validation and serialization
- **Redis Backend**: Redis/Redis Cluster integration with container types (RedisContainer, RedisList, RedisIndex)
- **Pipeline**: Wrapper around redpipe.Pipeline for batched Redis operations
- **Exceptions**: Custom exception hierarchy for field validation and missing field errors

## Development Commands

### Testing
```bash
# Run all tests using tox (recommended)
make test
# or
tox

# Run tests on specific Python versions
tox -e py39,py310,py311,py312

# Run single Python environment
tox -e py39

# Run comprehensive test script for all versions
./test_all_versions.sh

# Run tests with pytest directly  
pytest test/

# Run specific test file
python -m unittest test.test_definition

# Coverage reporting
coverage run --source hbom -m unittest discover -s test/
coverage report
```

### Code Quality
```bash
# Run flake8 linting
tox -e flake8-312

# Run flake8 on specific directory
tox -e flake8-312 -- hbom/
```

### Build and Distribution
```bash
# Build Cython extensions (optional optimization)
make local
# or with environment variable:
CYTHON_ENABLED=1 make local

# Create source distribution
make sdist

# Create wheel distribution  
make bdist

# Install locally
make install
```

### Cleanup
```bash
make clean      # Remove build artifacts
make cleancov    # Remove coverage files  
make cleanall    # Remove all temporary files
```

## Key Dependencies

- `redpipe>=4.2.0` - Redis pipeline operations (loaded from private GitHub repo)
- `future` - Legacy compatibility utilities (retained for some compatibility)
- `redis>=4.1.0` - Redis client
- Optional: `rediscluster` for Redis Cluster support
- Optional: `Cython>=0.23.4` for performance optimization

## Testing Environment

- Uses tox for testing across Python versions (py39, py310, py311, py312)
- Tests located in `test/` directory  
- Uses unittest framework with coverage reporting
- Redis backend testing requires redis server
- Includes flake8 linting environment (flake8-312)

## Python Version Support

- **Python 3.9+**: Minimum required version
- **Python 3.10**: Fully supported and tested (84/84 tests pass)
- **Python 3.11**: Fully supported and tested (84/84 tests pass)  
- **Python 3.12**: Fully supported and tested (84/84 tests pass)
- **Python 2.7**: Support removed - Python 3 only
- All dependencies are compatible with Python 3.9-3.12

### pyenv Setup for Development
For testing multiple Python versions, configure pyenv with:
```bash
pyenv global 3.9.18 3.10.18 3.11.5 3.12.11
```
This enables tox to find all Python interpreters for comprehensive testing.