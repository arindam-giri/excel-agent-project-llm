"""
Configuration management for Excel Agent
Uses pydantic-settings for type-safe configuration with environment variable support
"""

from pathlib import Path
from typing import Optional, Literal
from pydantic import Field, validator
from pydantic_settings import BaseSettings, SettingsConfigDict
import os


class Settings(BaseSettings):
    """
    Application settings with environment variable support
    
    Environment variables should be prefixed with EXCEL_AGENT_
    Example: EXCEL_AGENT_AWS_REGION=us-east-1
    """
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="EXCEL_AGENT_",
        case_sensitive=False,
        extra="ignore"
    )
    
    # ==================== Application Settings ====================
    app_name: str = Field(default="Excel Agent", description="Application name")
    app_version: str = Field(default="0.1.0", description="Application version")
    environment: Literal["development", "staging", "production"] = Field(
        default="development",
        description="Environment mode"
    )
    debug: bool = Field(default=True, description="Debug mode")
    log_level: str = Field(default="INFO", description="Logging level")
    
    # ==================== AWS Bedrock Settings ====================
    aws_region: str = Field(default="us-east-1", description="AWS region")
    aws_access_key_id: Optional[str] = Field(default=None, description="AWS access key")
    aws_secret_access_key: Optional[str] = Field(default=None, description="AWS secret key")
    aws_session_token: Optional[str] = Field(default=None, description="AWS session token")
    
    # Claude Model Settings
    claude_model_id: str = Field(
        default="anthropic.claude-3-sonnet-20240229",
        description="Claude model ID for Bedrock"
    )
    claude_max_tokens: int = Field(default=4096, description="Max tokens for Claude responses")
    claude_temperature: float = Field(default=0.0, description="Temperature for Claude (0.0-1.0)")
    claude_top_p: float = Field(default=0.999, description="Top-p sampling")
    claude_timeout: int = Field(default=300, description="Request timeout in seconds")
    
    # Prompt Caching
    enable_prompt_caching: bool = Field(
        default=True,
        description="Enable Claude prompt caching for repeated context"
    )
    
    # ==================== Path Settings ====================
    base_dir: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent,
        description="Project base directory"
    )
    
    # Data directories
    data_dir: Path = Field(default=None, description="Data directory")
    upload_dir: Path = Field(default=None, description="Upload directory")
    searchable_dir: Path = Field(default=None, description="Searchable CSV directory")
    cache_dir: Path = Field(default=None, description="Cache directory")
    log_dir: Path = Field(default=None, description="Log directory")
    
    @validator("data_dir", "upload_dir", "searchable_dir", "cache_dir", "log_dir", pre=True, always=True)
    def set_default_paths(cls, v, values, field):
        if v is None:
            base = values.get("base_dir", Path(__file__).parent.parent)
            path_map = {
                "data_dir": base / "data",
                "upload_dir": base / "data" / "uploads",
                "searchable_dir": base / "data" / "searchable",
                "cache_dir": base / "data" / "cache",
                "log_dir": base / "data" / "logs",
            }
            return path_map.get(field.name, base / "data")
        return Path(v)
    
    # ==================== File Processing Settings ====================
    max_upload_size_mb: int = Field(default=500, description="Max upload size in MB")
    allowed_extensions: list[str] = Field(
        default=[".xlsx", ".xls", ".xlsm"],
        description="Allowed Excel file extensions"
    )
    max_files_per_query: int = Field(default=10, description="Max files per query")
    
    # Excel Processing
    excel_read_only: bool = Field(default=True, description="Open Excel in read-only mode")
    excel_data_only: bool = Field(default=True, description="Read values only (not formulas)")
    max_rows_to_sample: int = Field(default=1000, description="Max rows to sample for indexing")
    max_sheet_preview_rows: int = Field(default=50, description="Rows to show in preview")
    
    # ==================== Search Settings ====================
    # Ripgrep
    ripgrep_binary: str = Field(default="rg", description="Ripgrep binary path")
    ripgrep_timeout: int = Field(default=30, description="Ripgrep timeout in seconds")
    enable_ripgrep: bool = Field(default=True, description="Enable ripgrep search")
    
    # Search behavior
    search_confidence_threshold: float = Field(
        default=0.3,
        description="Minimum confidence score for search results"
    )
    max_search_results: int = Field(default=100, description="Max search results to return")
    
    # Tokenization
    min_token_length: int = Field(default=2, description="Minimum token length")
    enable_fuzzy_matching: bool = Field(default=True, description="Enable fuzzy string matching")
    fuzzy_threshold: int = Field(default=80, description="Fuzzy matching threshold (0-100)")
    
    # ==================== Cache Settings ====================
    enable_cache: bool = Field(default=True, description="Enable caching")
    cache_ttl_seconds: int = Field(default=3600, description="Cache TTL in seconds")
    cache_size_limit_mb: int = Field(default=2048, description="Cache size limit in MB")
    
    # Cache strategies
    cache_excel_metadata: bool = Field(default=True, description="Cache Excel metadata")
    cache_search_results: bool = Field(default=True, description="Cache search results")
    cache_csv_files: bool = Field(default=True, description="Cache converted CSV files")
    
    # Memory cache
    memory_cache_size: int = Field(default=128, description="In-memory LRU cache size")
    
    # ==================== Agent Settings ====================
    # LangGraph
    max_agent_iterations: int = Field(default=10, description="Max agent iterations")
    agent_timeout_seconds: int = Field(default=300, description="Agent timeout")
    enable_agent_verbose: bool = Field(default=True, description="Verbose agent logging")
    
    # Tool execution
    enable_parallel_tool_execution: bool = Field(
        default=False,
        description="Enable parallel tool execution (experimental)"
    )
    max_parallel_tools: int = Field(default=3, description="Max parallel tools")
    
    # ==================== API Settings ====================
    api_host: str = Field(default="0.0.0.0", description="API host")
    api_port: int = Field(default=8000, description="API port")
    api_reload: bool = Field(default=True, description="Auto-reload on code changes")
    api_workers: int = Field(default=1, description="Number of worker processes")
    
    # CORS
    enable_cors: bool = Field(default=True, description="Enable CORS")
    cors_origins: list[str] = Field(
        default=["http://localhost:3000", "http://localhost:8000"],
        description="Allowed CORS origins"
    )
    
    # Rate limiting
    enable_rate_limiting: bool = Field(default=False, description="Enable rate limiting")
    rate_limit_per_minute: int = Field(default=60, description="Requests per minute")
    
    # ==================== Performance Settings ====================
    # Pandas optimization
    pandas_chunksize: int = Field(default=10000, description="Pandas read chunk size")
    max_dataframe_memory_mb: int = Field(
        default=512,
        description="Max memory for single DataFrame"
    )
    
    # Concurrent processing
    max_workers: int = Field(default=4, description="Max worker threads")
    
    # ==================== Feature Flags ====================
    enable_pivot_analysis: bool = Field(default=True, description="Enable pivot table analysis")
    enable_formula_tracking: bool = Field(default=False, description="Enable formula tracking")
    enable_chart_extraction: bool = Field(default=False, description="Enable chart extraction")
    
    # ==================== Monitoring & Observability ====================
    enable_metrics: bool = Field(default=False, description="Enable metrics collection")
    metrics_port: int = Field(default=9090, description="Metrics endpoint port")
    
    enable_tracing: bool = Field(default=False, description="Enable distributed tracing")
    
    # ==================== Security Settings ====================
    enable_file_validation: bool = Field(default=True, description="Validate uploaded files")
    enable_virus_scan: bool = Field(default=False, description="Enable virus scanning")
    max_filename_length: int = Field(default=255, description="Max filename length")
    
    # ==================== Methods ====================
    
    def create_directories(self) -> None:
        """Create all required directories if they don't exist"""
        directories = [
            self.data_dir,
            self.upload_dir,
            self.searchable_dir,
            self.cache_dir,
            self.log_dir,
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def validate_ripgrep(self) -> bool:
        """Check if ripgrep is available"""
        import shutil
        return shutil.which(self.ripgrep_binary) is not None
    
    def get_aws_credentials(self) -> dict:
        """Get AWS credentials for boto3"""
        creds = {}
        
        if self.aws_access_key_id:
            creds["aws_access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            creds["aws_secret_access_key"] = self.aws_secret_access_key
        if self.aws_session_token:
            creds["aws_session_token"] = self.aws_session_token
        
        creds["region_name"] = self.aws_region
        
        return creds
    
    def get_log_config(self) -> dict:
        """Get logging configuration"""
        return {
            "level": self.log_level,
            "format": "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                     "<level>{level: <8}</level> | "
                     "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                     "<level>{message}</level>",
            "serialize": False,
            "rotation": "100 MB",
            "retention": "7 days",
            "compression": "zip",
        }
    
    @property
    def max_upload_size_bytes(self) -> int:
        """Get max upload size in bytes"""
        return self.max_upload_size_mb * 1024 * 1024
    
    @property
    def cache_size_limit_bytes(self) -> int:
        """Get cache size limit in bytes"""
        return self.cache_size_limit_mb * 1024 * 1024
    
    def __repr__(self) -> str:
        """Custom repr to hide sensitive data"""
        return (
            f"Settings(environment={self.environment}, "
            f"aws_region={self.aws_region}, "
            f"claude_model={self.claude_model_id})"
        )


# Global settings instance
settings = Settings()

# Create directories on import
settings.create_directories()


# Validate critical dependencies
def validate_environment() -> dict[str, bool]:
    """
    Validate environment setup
    Returns dict of validation results
    """
    results = {
        "directories_exist": all([
            settings.data_dir.exists(),
            settings.upload_dir.exists(),
            settings.cache_dir.exists(),
        ]),
        "ripgrep_available": settings.validate_ripgrep() if settings.enable_ripgrep else True,
        "aws_credentials_set": bool(
            settings.aws_access_key_id and settings.aws_secret_access_key
        ) or bool(os.getenv("AWS_PROFILE")),
    }
    
    return results


if __name__ == "__main__":
    # Test configuration
    print("=" * 60)
    print("Excel Agent Configuration")
    print("=" * 60)
    print(f"Environment: {settings.environment}")
    print(f"Debug: {settings.debug}")
    print(f"AWS Region: {settings.aws_region}")
    print(f"Claude Model: {settings.claude_model_id}")
    print(f"Base Directory: {settings.base_dir}")
    print(f"Upload Directory: {settings.upload_dir}")
    print(f"Cache Directory: {settings.cache_dir}")
    print(f"Enable Ripgrep: {settings.enable_ripgrep}")
    print(f"Enable Cache: {settings.enable_cache}")
    print("=" * 60)
    
    # Validate environment
    validation = validate_environment()
    print("\nEnvironment Validation:")
    for check, passed in validation.items():
        status = "✓" if passed else "✗"
        print(f"  {status} {check}: {passed}")
    
    if not all(validation.values()):
        print("\n Some checks failed. Please review configuration.")
    else:
        print("\n✓ All environment checks passed!")