"""Logger module for analytics project.

This module re-exports the logger from utils_logger for use within the utils package.
"""

from analytics_project.utils_logger import get_log_file_path, init_logger, logger

__all__ = ["get_log_file_path", "init_logger", "logger"]
