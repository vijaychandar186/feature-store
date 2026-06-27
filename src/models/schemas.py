from pydantic import BaseModel, Field, field_validator
from typing import Any, Literal
from datetime import datetime
import uuid


# ------------------------------------------------------------------
# Feature type system
# ------------------------------------------------------------------

FeatureDtype = Literal["float64", "int64", "string", "bool", "list_float64", "list_int64", "bytes"]

DTYPE_PY_MAP: dict[str, type] = {
    "float64": float,
    "int64": int,
    "string": str,
    "bool": bool,
    "list_float64": list,
    "list_int64": list,
    "bytes": bytes,
}


class FeatureDefinition(BaseModel):
    name: str
    dtype: FeatureDtype
    description: str = ""
    default: Any = None
    tags: dict[str, str] = Field(default_factory=dict)


# ------------------------------------------------------------------
# Feature view
# ------------------------------------------------------------------

class FeatureViewSpec(BaseModel):
    name: str
    entity_column: str = "entity_id"
    timestamp_column: str = "event_timestamp"
    features: list[FeatureDefinition]
    ttl_seconds: int | None = None
    tags: dict[str, str] = Field(default_factory=dict)
    description: str = ""


class FeatureView(FeatureViewSpec):
    schema_version: int = 1
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = True


# ------------------------------------------------------------------
# Schema registry
# ------------------------------------------------------------------

CompatibilityMode = Literal["BACKWARD", "FORWARD", "FULL", "NONE"]


class SchemaVersion(BaseModel):
    feature_view: str
    version: int
    spec: FeatureViewSpec
    compatibility_mode: CompatibilityMode = "BACKWARD"
    is_latest: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)


class RegisterSchemaRequest(BaseModel):
    spec: FeatureViewSpec
    compatibility_mode: CompatibilityMode = "BACKWARD"


class CompatibilityCheckRequest(BaseModel):
    feature_view: str
    new_spec: FeatureViewSpec


class CompatibilityCheckResult(BaseModel):
    compatible: bool
    mode: CompatibilityMode
    errors: list[str]
    warnings: list[str]


# ------------------------------------------------------------------
# Feature values
# ------------------------------------------------------------------

class FeatureRow(BaseModel):
    entity_id: str
    features: dict[str, Any]
    event_timestamp: datetime = Field(default_factory=datetime.utcnow)


class WriteFeatureRequest(BaseModel):
    feature_view: str
    rows: list[FeatureRow]


class OnlineFeatureRequest(BaseModel):
    feature_view: str
    entity_ids: list[str]
    feature_names: list[str] | None = None  # None = all features


class OnlineFeatureResponse(BaseModel):
    feature_view: str
    results: dict[str, dict[str, Any]]   # entity_id → {feature: value}
    missing_entities: list[str]
    served_at: datetime = Field(default_factory=datetime.utcnow)


class HistoricalFeatureRequest(BaseModel):
    feature_view: str
    entity_timestamps: list[tuple[str, datetime]]   # (entity_id, as_of_time)
    feature_names: list[str] | None = None


class HistoricalFeatureRow(BaseModel):
    entity_id: str
    label_timestamp: datetime
    features: dict[str, Any] | None
    feature_timestamp: datetime | None


class HistoricalFeatureResponse(BaseModel):
    feature_view: str
    rows: list[HistoricalFeatureRow]
    retrieved_at: datetime = Field(default_factory=datetime.utcnow)


# ------------------------------------------------------------------
# Kafka messages
# ------------------------------------------------------------------

class FeatureUpdateMessage(BaseModel):
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    feature_view: str
    entity_id: str
    features: dict[str, Any]
    event_timestamp: datetime
    schema_version: int
    producer_id: str = "api"
    produced_at: datetime = Field(default_factory=datetime.utcnow)


class SchemaChangeMessage(BaseModel):
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    feature_view: str
    old_version: int | None
    new_version: int
    change_type: Literal["create", "update", "delete"]
    produced_at: datetime = Field(default_factory=datetime.utcnow)


# ------------------------------------------------------------------
# Materialization
# ------------------------------------------------------------------

class MaterializationJob(BaseModel):
    job_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    feature_view: str
    start_time: datetime | None = None
    end_time: datetime | None = None
    status: Literal["pending", "running", "completed", "failed"] = "pending"
    entities_materialized: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: datetime | None = None
    error: str | None = None
