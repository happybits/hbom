#!/bin/bash

# Comprehensive test script for all Python versions

set -e  # Exit on any error

echo "🐍 Python Version Test Suite for HBOM"
echo "======================================"

# Check available Python versions
echo "📋 Checking available Python versions..."
for version in "3.9" "3.10" "3.11" "3.12"; do
    if command -v python${version} &> /dev/null; then
        echo "✅ Python ${version}: $(python${version} --version)"
    else
        echo "❌ Python ${version}: Not available"
    fi
done

echo ""
echo "🧪 Testing individual tox environments..."

# Test each Python environment individually
for env in py39 py310 py311 py312; do
    echo "Testing ${env}..."
    if tox -e ${env}; then
        echo "✅ ${env}: PASSED"
    else
        echo "❌ ${env}: FAILED"
    fi
    echo ""
done

# Test flake8
echo "🔍 Testing flake8..."
if tox -e flake8-312; then
    echo "✅ flake8-312: PASSED"
else
    echo "❌ flake8-312: FAILED"
fi

echo ""
echo "🏃 Running full tox suite..."
if tox; then
    echo "✅ Full tox suite: PASSED"
else
    echo "❌ Full tox suite: FAILED"
fi

echo ""
echo "🔨 Testing Makefile commands..."
if make test; then
    echo "✅ make test: PASSED"
else
    echo "❌ make test: FAILED"
fi

echo ""
echo "🎉 Testing complete!"
