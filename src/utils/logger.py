"""
Professional Logging Framework for Retail Analytics Platform
Provides structured logging with multiple handlers and formatters
"""
import logging
import logging.handlers
import sys
import os
from datetime import datetime
from pathlib import Path
from typing import Optional
import json

from config.settings import config


class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging"""
    
    def format(self, record):
        """Format log record as JSON"""
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields if present
        if hasattr(record, 'extra_fields'):
            log_entry.update(record.extra_fields)
        
        return json.dumps(log_entry)


class PipelineLogger:
    """Enhanced logger for data pipeline operations"""
    
    def __init__(self, name: str, log_file: Optional[str] = None):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, config.logging.level))
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Setup handlers
        self._setup_console_handler()
        self._setup_file_handler(log_file)
        
        # Prevent duplicate logs
        self.logger.propagate = False
    
    def _setup_console_handler(self):
        """Setup console handler with colored output"""
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # Colored formatter for console
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
    
    def _setup_file_handler(self, log_file: Optional[str] = None):
        """Setup rotating file handler with JSON formatting"""
        log_dir = Path(config.storage.get_absolute_path(config.storage.log_path))
        log_dir.mkdir(exist_ok=True)
        
        if log_file is None:
            log_file = f"{self.logger.name}_{datetime.now().strftime('%Y%m%d')}.log"
        
        log_path = log_dir / log_file
        
        # Rotating file handler
        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=config.logging.max_file_size,
            backupCount=config.logging.backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        
        # JSON formatter for file
        json_formatter = JsonFormatter()
        file_handler.setFormatter(json_formatter)
        self.logger.addHandler(file_handler)
    
    def info(self, message: str, **kwargs):
        """Log info message with optional extra fields"""
        extra = {'extra_fields': kwargs} if kwargs else {}
        self.logger.info(message, extra=extra)
    
    def debug(self, message: str, **kwargs):
        """Log debug message with optional extra fields"""
        extra = {'extra_fields': kwargs} if kwargs else {}
        self.logger.debug(message, extra=extra)
    
    def warning(self, message: str, **kwargs):
        """Log warning message with optional extra fields"""
        extra = {'extra_fields': kwargs} if kwargs else {}
        self.logger.warning(message, extra=extra)
    
    def error(self, message: str, **kwargs):
        """Log error message with optional extra fields"""
        extra = {'extra_fields': kwargs} if kwargs else {}
        self.logger.error(message, extra=extra)
    
    def critical(self, message: str, **kwargs):
        """Log critical message with optional extra fields"""
        extra = {'extra_fields': kwargs} if kwargs else {}
        self.logger.critical(message, extra=extra)
    
    def log_pipeline_start(self, pipeline_name: str, **metadata):
        """Log pipeline start with metadata"""
        self.info(
            f"🚀 Pipeline started: {pipeline_name}",
            pipeline_name=pipeline_name,
            event_type="pipeline_start",
            **metadata
        )
    
    def log_pipeline_end(self, pipeline_name: str, duration: float, status: str, **metadata):
        """Log pipeline completion with metrics"""
        self.info(
            f"✅ Pipeline completed: {pipeline_name} in {duration:.2f}s - Status: {status}",
            pipeline_name=pipeline_name,
            duration_seconds=duration,
            status=status,
            event_type="pipeline_end",
            **metadata
        )
    
    def log_data_quality(self, table_name: str, quality_metrics: dict):
        """Log data quality metrics"""
        self.info(
            f"📊 Data quality check: {table_name}",
            table_name=table_name,
            event_type="data_quality",
            **quality_metrics
        )
    
    def log_performance_metrics(self, operation: str, metrics: dict):
        """Log performance metrics"""
        self.info(
            f"⚡ Performance metrics: {operation}",
            operation=operation,
            event_type="performance_metrics",
            **metrics
        )


def get_logger(name: str, log_file: Optional[str] = None) -> PipelineLogger:
    """
    Factory function to create pipeline logger
    
    Args:
        name: Logger name (usually module name)
        log_file: Optional specific log file name
    
    Returns:
        PipelineLogger instance
    """
    return PipelineLogger(name, log_file)


# Decorator for automatic function logging
def log_execution_time(logger: PipelineLogger):
    """Decorator to log function execution time"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            import time
            start_time = time.time()
            
            logger.info(f"🔄 Starting {func.__name__}")
            
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                
                logger.info(
                    f"✅ Completed {func.__name__} in {duration:.2f}s",
                    function_name=func.__name__,
                    duration_seconds=duration,
                    status="success"
                )
                
                return result
                
            except Exception as e:
                duration = time.time() - start_time
                
                logger.error(
                    f"❌ Failed {func.__name__} after {duration:.2f}s: {str(e)}",
                    function_name=func.__name__,
                    duration_seconds=duration,
                    status="failed",
                    error_message=str(e)
                )
                raise
        
        return wrapper
    return decorator


# Create global logger instance
logger = PipelineLogger("pipeline")
