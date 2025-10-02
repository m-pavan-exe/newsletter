"""
Utility modules for the AI Summary Framework.

This package contains various utility modules that provide common functionality
across the application, including logging configuration and error handling.
"""

from .logging_config import setup_logging, get_logger
from .error_handling import (
    AIFrameworkError,
    DatabaseError,
    ConfigurationError,
    ValidationError,
    handle_database_errors,
    handle_validation_errors,
    ErrorContext,
    log_errors
)

__all__ = [
    'setup_logging',
    'get_logger',
    'AIFrameworkError',
    'DatabaseError',
    'ConfigurationError',
    'ValidationError',
    'handle_database_errors',
    'handle_validation_errors',
    'ErrorContext',
    'log_errors',
]
