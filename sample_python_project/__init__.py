"""Sample Python Project.

A simple Python package to demonstrate release-please integration.
"""

from .version import __version__

__all__ = ["__version__", "greet", "calculate"]


def greet(name: str) -> str:
    """Return a greeting message.

    Args:
        name: The name to greet.

    Returns:
        A greeting message.
    """
    return f"Hello, {name}! Welcome to sample_python_project v{__version__}"


def calculate(a: int, b: int, operation: str = "add") -> int:
    """Perform a calculation.

    Args:
        a: First number.
        b: Second number.
        operation: The operation to perform (add, subtract, multiply, divide).

    Returns:
        The result of the calculation.

    Raises:
        ValueError: If the operation is not supported.
        ZeroDivisionError: If trying to divide by zero.
    """
    if operation == "add":
        return a + b
    elif operation == "subtract":
        return a - b
    elif operation == "multiply":
        return a * b
    elif operation == "divide":
        if b == 0:
            raise ZeroDivisionError("Cannot divide by zero")
        return a // b
    else:
        raise ValueError(f"Unsupported operation: {operation}")
