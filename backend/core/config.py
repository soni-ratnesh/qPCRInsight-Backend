# backend/core/config.py

from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables.
    
    Attributes:
        RAW_BUCKET_NAME: S3 bucket for raw uploaded files
        REPORT_BUCKET_NAME: S3 bucket for generated reports
        JOB_TABLE_NAME: DynamoDB table for job tracking
        COGNITO_POOL_ID: Cognito user pool ID
        COGNITO_CLIENT_ID: Cognito app client ID
        REGION: AWS region
        LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    
    RAW_BUCKET_NAME: Optional[str] = None
    REPORT_BUCKET_NAME: Optional[str] = None
    JOB_TABLE_NAME: Optional[str] = None
    COGNITO_POOL_ID: Optional[str] = None
    COGNITO_CLIENT_ID: Optional[str] = None
    REGION: str = "us-east-1"
    LOG_LEVEL: str = "INFO"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance (lazy singleton).
    
    Returns:
        Settings: Application settings instance
    """
    return Settings()