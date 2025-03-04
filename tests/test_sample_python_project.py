"""Tests for the sample_python_project package."""

import pytest

from sample_python_project import greet, calculate, __version__


def test_version():
    """Test that the version is a string."""
    assert isinstance(__version__, str)
    assert __version__ != ""


def test_greet():
    """Test the greet function."""
    result = greet("Test")
    assert isinstance(result, str)
    assert "Hello, Test!" in result
    assert __version__ in result


def test_calculate_add():
    """Test the calculate function with addition."""
    result = calculate(5, 3, "add")
    assert result == 8


def test_calculate_subtract():
    """Test the calculate function with subtraction."""
    result = calculate(5, 3, "subtract")
    assert result == 2


def test_calculate_multiply():
    """Test the calculate function with multiplication."""
    result = calculate(5, 3, "multiply")
    assert result == 15


def test_calculate_divide():
    """Test the calculate function with division."""
    result = calculate(6, 3, "divide")
    assert result == 2


def test_calculate_divide_by_zero():
    """Test the calculate function with division by zero."""
    with pytest.raises(ZeroDivisionError):
        calculate(5, 0, "divide")


def test_calculate_invalid_operation():
    """Test the calculate function with an invalid operation."""
    with pytest.raises(ValueError):
        calculate(5, 3, "invalid")
