"""
Configuration settings for Azure services
"""

from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings"""
    
    # Azure OpenAI Configuration
    AZURE_OPENAI_ENDPOINT: str
    AZURE_OPENAI_API_KEY: str
    AZURE_OPENAI_DEPLOYMENT_NAME: str = "gpt-4o"
    AZURE_OPENAI_API_VERSION: str = "2024-12-01-preview"
    
    # Azure Blob Storage Configuration
    AZURE_STORAGE_CONNECTION_STRING: str
    AZURE_STORAGE_CONTAINER_JOB_DESCRIPTIONS: str = "job-descriptions"
    AZURE_STORAGE_CONTAINER_RESUMES: str = "resumes"
    
    # Azure Cosmos DB Configuration
    COSMOS_DB_ENDPOINT: str
    COSMOS_DB_KEY: str
    COSMOS_DB_DATABASE_NAME: str = "resume-screening"
    COSMOS_DB_CONTAINER_JOBS: str = "jobs"
    COSMOS_DB_CONTAINER_SCREENINGS: str = "screenings"
    COSMOS_DB_CONTAINER_USERS: str = "users"
    
    # JWT Authentication Configuration
    JWT_SECRET_KEY: str = "your-secret-key-change-this-in-production-use-env-variable"
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440  # 24 hours
    
    # Application Settings
    MAX_FILE_SIZE_MB: int = 10
    ALLOWED_EXTENSIONS: list = [".pdf", ".docx", ".doc"]
    
    # AI Processing Settings
    MIN_FIT_SCORE_FOR_INTERVIEW: int = 60
    TOP_SKILLS_FOR_DEPTH_ANALYSIS: int = 6
    MAX_RESUMES_PER_BATCH: int = 50
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()