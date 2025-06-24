# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

HBOM (Happy Bits Object Model) is a Python library that provides a Redis-backed object model, extending OMA (Object Model Architecture). It uses redpipe for Redis pipeline operations and supports both Redis and Redis Cluster backends.

## Quick Reference

### Essential Commands
```bash
make test          # Run all tests
make clean         # Clean build artifacts
make sdist         # Build source distribution
make bdist         # Build wheel
```

### Model Creation Pattern
```python
# Define model
class User(hbom.RedisObject):
    class definition(hbom.Definition):
        id = hbom.StringField(primary=True)
        name = hbom.StringField(required=True)
    _db = 'test'

# Create and save
user = User.new(id='123', name='Alice')
User.save(user)

# Retrieve
user = User.get('123')
```

## Core Architecture

### Modules
- **`definition.py`**: Base metaclass system (`Definition`) for defining models with field validation and primary key enforcement
- **`fields.py`**: Field types for model attributes with validation and serialization
- **`redis_backend.py`**: Redis/Redis Cluster integration with container types and model persistence
- **`pipeline.py`**: Wrapper around redpipe.Pipeline for batched Redis operations
- **`exceptions.py`**: Custom exception hierarchy (FieldError, InvalidFieldValue, MissingField, InvalidOperation)
- **`compat.py`**: JSON compatibility layer (ujson with fallback to standard json)

### Key Classes and Patterns

#### Model Definition Pattern
```python
class MyModel(hbom.RedisObject):
    class definition(hbom.Definition):
        id = hbom.StringField(primary=True)
        name = hbom.StringField(required=True)
        count = hbom.IntegerField(default=0)
    
    _keyspace = 'MyModel'  # Redis key prefix
    _db = 'test'          # Redis connection name
```

#### Field Types
- **StringField**: ASCII strings (use TextField for unicode)
- **TextField**: Unicode text with full character support
- **IntegerField**: Integer values
- **FloatField**: Floating point numbers
- **BooleanField**: Boolean values (default=False)
- **DictField**: JSON-serializable dictionaries
- **ListField**: JSON-serializable lists
- **StringListField**: Lists of strings with None filtering

#### Redis Container Types
- **RedisString**: Simple key-value storage
- **RedisList**: List operations (lpush, rpop, etc.)
- **RedisSet**: Set operations (sadd, srem, etc.)
- **RedisSortedSet**: Sorted set with scores
- **RedisHash**: Hash/dictionary storage
- **RedisDistributedHash**: Sharded hash for large datasets
- **RedisIndex**: Sharded index for lookups

## Important Patterns and Gotchas

### Model Creation and Saving
```python
# CORRECT: Use Model.new() to create instances
model = MyModel.new(name="test")

# WRONG: Don't use constructor directly
# model = MyModel(name="test")  # This won't work!

# CORRECT: Save using class method
MyModel.save(model)

# WRONG: No instance save method
# model.save()  # This doesn't exist!
```

### Field Behavior
- **Primary keys**: Cannot be modified after creation (raises InvalidOperation)
- **Required fields**: Empty string is valid, None is not (raises MissingField)
- **Field tracking**: Uses `_dirty` set internally (not `_changed`)
- **StringListField**: Empty lists may become None
- **Default values**: May not always be returned immediately (field-specific behavior)

### Redis Operations
- **Batch operations**: Prefer `get_multi()` for multiple keys
- **Pipeline usage**: Use `with Pipeline(autoexec=True)` for automatic execution
- **Connection naming**: Specify `_db` in model class for connection selection
- **Key patterns**: `{keyspace}:{primary_key}` format

### Python 3 Compatibility
- **String encoding**: Required for operations like `hashlib.md5(value.encode('utf-8'))`
- **Bytes handling**: Redis returns bytes, may need decoding
- **No Python 2 support**: Removed as of version 0.14.0

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

# Run flake8 directly on all code
flake8 --max-complexity=15 --exclude=./build,.env,.venv,.tox,dist,./test/ --ignore=F403 --max-line-length=99 hbom/
flake8 --max-complexity=15 --max-line-length=99 test/
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
- Tests located in `test/` directory (114 tests total)
- Uses unittest framework with coverage reporting (90% coverage)
- Redis backend testing requires redis server (uses redislite for local testing)
- Includes flake8 linting environment (flake8-312)

### Test Patterns
```python
# Test models must specify Redis connection
class TestModel(hbom.RedisObject):
    class definition(hbom.Definition):
        id = hbom.StringField(primary=True)
    
    _db = 'test'  # Required for test models

# Clear test data in setUp/tearDown
def setUp(self):
    clear_redis_testdata()

def tearDown(self):
    clear_redis_testdata()

# Skip Redis tests when not available
@skip_if_redis_disabled
def test_redis_operation(self):
    pass
```

### Edge Cases Tested
- Unicode and special characters (emojis, symbols)
- Very long strings (10k+ characters)
- Numeric boundary values (zero, negative, max values)
- Empty vs None distinction
- Concurrent modifications
- Large datasets (100k+ items)
- Redis connection failures
- Pipeline execution errors

## Python Version Support

- **Python 3.9+**: Minimum required version
- **Python 3.10**: Fully supported and tested (114/114 tests pass)
- **Python 3.11**: Fully supported and tested (114/114 tests pass)  
- **Python 3.12**: Fully supported and tested (114/114 tests pass)
- **Python 2.7**: Support removed - Python 3 only
- All dependencies are compatible with Python 3.9-3.12

### pyenv Setup for Development
For testing multiple Python versions, configure pyenv with:
```bash
pyenv global 3.9.18 3.10.18 3.11.5 3.12.11
```
This enables tox to find all Python interpreters for comprehensive testing.

## Advanced Features

### Cold Storage Support
```python
class MyModel(hbom.RedisColdStorageObject):
    class definition(hbom.Definition):
        id = hbom.StringField(primary=True)
        data = hbom.TextField()
    
    coldstorage = MyColdStorageBackend()  # External storage
    freeze_ttl = 300  # Seconds before freezing to cold storage
    
    @classmethod
    def is_hot_key(cls, key):
        # Define keys that should never go to cold storage
        return key.startswith('hot_')
```

Cold storage features:
- Automatic archival of inactive data
- Hot key protection
- Transparent thaw on access
- MySQL blob truncation protection

### Distributed Hash Sharding
```python
class MyDistributedHash(hbom.RedisDistributedHash):
    _shards = 1000  # Number of shards for distribution
    _db = 'test'
```

### Reference Loading
```python
# Create reference without loading
ref = MyModel.ref('key_id')

# Load later when needed
ref.attach(pipe)  # Or use get() to load immediately
```

## Known Issues and Workarounds

### Deprecation Warnings
1. **hmset warning**: `DeprecationWarning: Call to deprecated hmset`
   - From redis client, doesn't affect functionality
   - Will be fixed in future redpipe update

2. **setuptools warning**: When using `python setup.py` directly
   - Use `python -m build` or `pip install .` instead
   - Modern packaging standards recommended

3. **pkg_resources warning**: In Python 3.12 environments
   - From redislite dependency
   - Doesn't affect functionality

### Common Pitfalls
1. **Field defaults**: May not behave as expected
   ```python
   # StringListField with default=[] may return None
   field = hbom.StringListField(default=[])
   # Check for None before using
   ```

2. **Empty keys**: Different behavior for None vs empty string
   ```python
   Model.get(None)   # May raise error or return instance
   Model.get("")     # Returns None or non-existent instance
   ```

3. **Pipeline execution**: Must be within context or explicitly executed
   ```python
   pipe = Pipeline()
   # ... operations ...
   pipe.execute()  # Don't forget this!
   ```

## Development Best Practices

1. **Always test with multiple Python versions** using tox
2. **Clear Redis test data** between test runs
3. **Use flake8** to maintain code quality
4. **Prefer batch operations** for performance
5. **Handle bytes/string conversion** explicitly for Python 3
6. **Document field validation requirements** clearly
7. **Use type hints** where appropriate (Python 3.9+)

## Debugging Tips

1. **Redis connection issues**: Check `_db` attribute on models
2. **Field validation errors**: Enable debug logging to see validation steps
3. **Pipeline not executing**: Ensure autoexec=True or explicit execute()
4. **Missing data**: Check if data is in cold storage
5. **Encoding errors**: Ensure proper string encoding for Python 3