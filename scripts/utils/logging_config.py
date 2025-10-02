"""
Logging configuration for the AI Summary Framework.

This module provides a centralized logging configuration that can be imported and
used across all modules in the application.
"""
import os
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional
from datetime import datetime

def setup_logging(
    log_dir: str = "logs",
    log_level: int = logging.INFO,
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
    console_output: bool = True
) -> None:
    """
    Set up logging configuration for the application.

    Args:
        log_dir: Directory to store log files
        log_level: Logging level (default: logging.INFO)
        max_bytes: Maximum size of log file before rotation
        backup_count: Number of backup log files to keep
        console_output: Whether to output logs to console
    """
    # Create log directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Generate log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"ai_summary_{timestamp}.log")
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # Clear any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler
    file_handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler
    if console_output:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # Disable overly verbose loggers
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy').setLevel(logging.WARNING)
    logging.getLogger('openai').setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the given name.
    
    Args:
        name: Name of the logger (usually __name__)
        
    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)
