"""
Error handling utilities for the AI Summary Framework.

This module provides decorators and context managers for consistent error handling
across the application.
"""
import functools
import logging
import sys
from typing import Any, Callable, Type, TypeVar, Optional, Dict, List
from typing_extensions import ParamSpec

# Type variables for generic function typing
P = ParamSpec('P')
T = TypeVar('T')

class AIFrameworkError(Exception):
    """Base exception class for all framework-specific exceptions."""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)

class DatabaseError(AIFrameworkError):
    """Raised when there are issues with database operations."""
    pass

class ConfigurationError(AIFrameworkError):
    """Raised when there are issues with configuration."""
    pass

class ValidationError(AIFrameworkError):
    """Raised when input validation fails."""
    pass

def handle_database_errors(func: Callable[P, T]) -> Callable[P, T]:
    """
    Decorator to handle database-related errors.
    
    Args:
        func: Function to be decorated
        
    Returns:
        Wrapped function with error handling
    """
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_msg = f"Database operation failed in {func.__name__}"
            logging.exception(f"{error_msg}: {str(e)}")
            raise DatabaseError(
                message=error_msg,
                details={
                    "function": func.__name__,
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                }
            ) from e
    return wrapper

def handle_validation_errors(func: Callable[P, T]) -> Callable[P, T]:
    """
    Decorator to handle validation errors.
    
    Args:
        func: Function to be decorated
        
    Returns:
        Wrapped function with error handling
    """
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return func(*args, **kwargs)
        except ValueError as e:
            error_msg = f"Validation failed in {func.__name__}"
            logging.warning(f"{error_msg}: {str(e)}")
            raise ValidationError(
                message=error_msg,
                details={
                    "function": func.__name__,
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                }
            ) from e
    return wrapper

class ErrorContext:
    """Context manager for error handling with custom error types."""
    
    def __init__(
        self,
        error_message: str,
        error_type: Type[Exception] = AIFrameworkError,
        log_level: int = logging.ERROR,
        reraise: bool = True
    ):
        """
        Initialize the error context.
        
        Args:
            error_message: Base error message
            error_type: Exception type to raise
            log_level: Logging level for the error
            reraise: Whether to re-raise the exception
        """
        self.error_message = error_message
        self.error_type = error_type
        self.log_level = log_level
        self.reraise = reraise
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # Format the error message with exception details
            full_message = f"{self.error_message}: {str(exc_val) if exc_val else 'Unknown error'}"
            
            # Log the error
            logger = logging.getLogger(self.error_type.__module__)
            logger.log(
                self.log_level,
                full_message,
                exc_info=(exc_type, exc_val, exc_tb)
            )
            
            # Optionally re-raise with the specified error type
            if self.reraise:
                raise self.error_type(full_message) from exc_val
            
            # Return True to suppress the exception if not re-raising
            return not self.reraise
        return False

def log_errors(
    error_message: str,
    error_type: Type[Exception] = AIFrameworkError,
    log_level: int = logging.ERROR,
    reraise: bool = True
):
    """
    Decorator factory for error handling with logging.
    
    Args:
        error_message: Base error message
        error_type: Exception type to raise
        log_level: Logging level for the error
        reraise: Whether to re-raise the exception
        
    Returns:
        Decorator function
    """
    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                with ErrorContext(error_message, error_type, log_level, reraise):
                    raise
        return wrapper
    return decorator
