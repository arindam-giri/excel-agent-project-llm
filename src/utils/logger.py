"""
Logging configuration for Excel Agent
Uses loguru for enhanced logging with automatic log rotation, formatting, and context
"""

import sys
from pathlib import Path
from typing import Optional
from loguru import logger
from config.settings import settings


class LoggerSetup:
    """Configure and manage application logging"""
    
    _initialized = False
    
    @classmethod
    def setup(cls, 
              log_file: Optional[str] = None,
              rotation: str = "100 MB",
              retention: str = "7 days",
              compression: str = "zip") -> None:
        """
        Setup logging configuration
        
        Args:
            log_file: Custom log file name (default: excel_agent.log)
            rotation: When to rotate logs (size or time based)
            retention: How long to keep logs
            compression: Compression format for rotated logs
        """
        if cls._initialized:
            logger.warning("Logger already initialized, skipping setup")
            return
        
        # Remove default handler
        logger.remove()
        
        # Console handler (stdout) - formatted for human reading
        logger.add(
            sys.stdout,
            format=cls._get_console_format(),
            level=settings.log_level,
            colorize=True,
            backtrace=settings.debug,
            diagnose=settings.debug,
        )
        
        # File handler - detailed logs with rotation
        log_path = settings.log_dir / (log_file or "excel_agent.log")
        logger.add(
            log_path,
            format=cls._get_file_format(),
            level="DEBUG",  # Always log DEBUG to file
            rotation=rotation,
            retention=retention,
            compression=compression,
            backtrace=True,
            diagnose=True,
            enqueue=True,  # Thread-safe
        )
        
        # Error file handler - separate file for errors
        error_log_path = settings.log_dir / "errors.log"
        logger.add(
            error_log_path,
            format=cls._get_file_format(),
            level="ERROR",
            rotation="50 MB",
            retention="30 days",
            compression="zip",
            backtrace=True,
            diagnose=True,
            enqueue=True,
        )
        
        # Performance log (optional) - for tracking slow operations
        if settings.enable_metrics:
            perf_log_path = settings.log_dir / "performance.log"
            logger.add(
                perf_log_path,
                format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {extra[operation]} | {extra[duration_ms]}ms | {message}",
                level="INFO",
                rotation="50 MB",
                retention="7 days",
                filter=lambda record: "performance" in record["extra"],
                enqueue=True,
            )
        
        cls._initialized = True
        logger.info(f"Logging initialized - Level: {settings.log_level}, Log dir: {settings.log_dir}")
    
    @staticmethod
    def _get_console_format() -> str:
        """Format for console output - clean and readable"""
        if settings.debug:
            return (
                "<green>{time:HH:mm:ss.SSS}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                "<level>{message}</level>"
            )
        else:
            return (
                "<green>{time:HH:mm:ss}</green> | "
                "<level>{level: <8}</level> | "
                "<level>{message}</level>"
            )
    
    @staticmethod
    def _get_file_format() -> str:
        """Format for file output - detailed with all context"""
        return (
            "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
            "{level: <8} | "
            "{process.name}:{process.id} | "
            "{thread.name}:{thread.id} | "
            "{name}:{function}:{line} | "
            "{message}"
        )
    
    @staticmethod
    def get_logger(name: str):
        """
        Get a logger instance for a specific module
        
        Args:
            name: Module name (usually __name__)
            
        Returns:
            Logger instance bound to the module name
        """
        return logger.bind(module=name)


# Context managers for structured logging

class LogContext:
    """Context manager for logging operations with timing"""
    
    def __init__(self, operation: str, level: str = "INFO", **kwargs):
        """
        Args:
            operation: Operation name
            level: Log level
            **kwargs: Additional context to log
        """
        self.operation = operation
        self.level = level
        self.context = kwargs
        self.start_time = None
    
    def __enter__(self):
        import time
        self.start_time = time.time()
        logger.bind(**self.context).log(
            self.level, 
            f"Starting: {self.operation}"
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        import time
        duration_ms = (time.time() - self.start_time) * 1000
        
        if exc_type is None:
            logger.bind(**self.context).log(
                self.level,
                f"Completed: {self.operation} ({duration_ms:.2f}ms)"
            )
        else:
            logger.bind(**self.context).error(
                f"Failed: {self.operation} ({duration_ms:.2f}ms) - {exc_val}"
            )
        
        # Log performance if enabled
        if settings.enable_metrics:
            logger.bind(
                performance=True,
                operation=self.operation,
                duration_ms=duration_ms
            ).info(f"Performance metric")
        
        return False  # Don't suppress exceptions


class LogPerformance:
    """Decorator for logging function performance"""
    
    def __init__(self, operation: Optional[str] = None):
        self.operation = operation
    
    def __call__(self, func):
        from functools import wraps
        import time
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            operation = self.operation or func.__name__
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                duration_ms = (time.time() - start_time) * 1000
                
                logger.debug(f"{operation} completed in {duration_ms:.2f}ms")
                
                if settings.enable_metrics:
                    logger.bind(
                        performance=True,
                        operation=operation,
                        duration_ms=duration_ms
                    ).info("Performance metric")
                
                return result
            
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                logger.error(
                    f"{operation} failed after {duration_ms:.2f}ms: {e}"
                )
                raise
        
        return wrapper


# Utility functions

def log_function_call(func_name: str, args: tuple, kwargs: dict) -> None:
    """Log function call with arguments (for debugging)"""
    if settings.debug:
        args_str = ", ".join([repr(arg) for arg in args])
        kwargs_str = ", ".join([f"{k}={repr(v)}" for k, v in kwargs.items()])
        all_args = ", ".join(filter(None, [args_str, kwargs_str]))
        logger.debug(f"Calling {func_name}({all_args})")


def log_dataframe_info(df, name: str = "DataFrame") -> None:
    """Log pandas DataFrame information"""
    logger.debug(
        f"{name} info: shape={df.shape}, "
        f"columns={list(df.columns)}, "
        f"memory={df.memory_usage(deep=True).sum() / 1024**2:.2f}MB"
    )


def log_file_operation(operation: str, file_path: Path, **kwargs) -> None:
    """Log file operations"""
    logger.info(
        f"File {operation}: {file_path.name}",
        extra={"file_path": str(file_path), "operation": operation, **kwargs}
    )


def log_search_results(query: str, result_count: int, duration_ms: float, method: str) -> None:
    """Log search operation results"""
    logger.info(
        f"Search completed: '{query}' -> {result_count} results ({duration_ms:.2f}ms) [method: {method}]"
    )


def log_agent_step(step: str, iteration: int, tool: Optional[str] = None) -> None:
    """Log agent workflow steps"""
    if tool:
        logger.info(f"Agent[{iteration}] - {step}: using tool '{tool}'")
    else:
        logger.info(f"Agent[{iteration}] - {step}")


def log_cache_operation(operation: str, key: str, hit: bool = None, size_bytes: int = None) -> None:
    """Log cache operations"""
    extra = {"cache_key": key, "operation": operation}
    if hit is not None:
        extra["cache_hit"] = hit
    if size_bytes is not None:
        extra["size_kb"] = size_bytes / 1024
    
    status = "HIT" if hit else "MISS" if hit is not None else "SET"
    logger.debug(f"Cache {status}: {key[:50]}...", extra=extra)


def log_error_with_context(error: Exception, context: dict) -> None:
    """Log error with additional context"""
    logger.error(
        f"Error: {type(error).__name__}: {str(error)}",
        extra={"error_type": type(error).__name__, **context}
    )
    if settings.debug:
        logger.exception("Full traceback:")


# Specialized loggers for different components

class ComponentLogger:
    """Create specialized loggers for different components"""
    
    @staticmethod
    def search_logger():
        return logger.bind(component="search")
    
    @staticmethod
    def agent_logger():
        return logger.bind(component="agent")
    
    @staticmethod
    def cache_logger():
        return logger.bind(component="cache")
    
    @staticmethod
    def api_logger():
        return logger.bind(component="api")
    
    @staticmethod
    def preprocessing_logger():
        return logger.bind(component="preprocessing")
    
    @staticmethod
    def tool_logger(tool_name: str):
        return logger.bind(component="tool", tool=tool_name)


# Initialize logging on import
LoggerSetup.setup()


# Export commonly used items
__all__ = [
    'logger',
    'LoggerSetup',
    'LogContext',
    'LogPerformance',
    'log_function_call',
    'log_dataframe_info',
    'log_file_operation',
    'log_search_results',
    'log_agent_step',
    'log_cache_operation',
    'log_error_with_context',
    'ComponentLogger',
]


# Example usage
if __name__ == "__main__":
    # Basic logging
    logger.info("This is an info message")
    logger.debug("This is a debug message")
    logger.warning("This is a warning")
    logger.error("This is an error")
    
    # With context manager
    with LogContext("test_operation", level="INFO", user_id=123):
        logger.info("Doing some work...")
        import time
        time.sleep(0.1)
    
    # Performance decorator
    @LogPerformance("complex_calculation")
    def calculate_something():
        import time
        time.sleep(0.05)
        return 42
    
    result = calculate_something()
    
    # Specialized logging
    search_log = ComponentLogger.search_logger()
    search_log.info("Search completed successfully")
    
    # File operation
    log_file_operation("upload", Path("/tmp/test.xlsx"), size_mb=10)
    
    # Search results
    log_search_results("revenue 2024", 25, 123.45, "ripgrep")
    
    # Cache operation
    log_cache_operation("get", "excel_metadata_abc123", hit=True, size_bytes=1024)
    
    print("\nCheck logs in:", settings.log_dir)