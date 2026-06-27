from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "feature_store"
    postgres_user: str = "fsuser"
    postgres_password: str = "fspassword"
    postgres_min_pool: int = 2
    postgres_max_pool: int = 20

    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: str = ""
    redis_db: int = 0
    redis_ttl_seconds: int = 86400

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_feature_updates_topic: str = "feature-updates"
    kafka_schema_changes_topic: str = "schema-changes"
    kafka_consumer_group: str = "feature-store-consumer"
    kafka_auto_offset_reset: str = "earliest"

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # Materialization
    materialization_batch_size: int = 1000
    online_feature_ttl_seconds: int = 86400

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def asyncpg_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
