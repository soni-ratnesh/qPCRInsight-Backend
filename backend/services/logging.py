# backend/services/logging.py

import logging
from functools import lru_cache

from backend.core.config import get_settings


@lru_cache()
def get_logger(name: str) -> logging.Logger:
    """Get configured logger instance.
    
    Args:
        name: Logger name
        
    Returns:
        logging.Logger: Configured logger
    """
    settings = get_settings()
    logger = logging.getLogger(name)
    logger.setLevel(settings.LOG_LEVEL)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger