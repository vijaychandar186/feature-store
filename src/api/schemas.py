from fastapi import APIRouter, HTTPException
from src.models.schemas import (
    RegisterSchemaRequest,
    CompatibilityCheckRequest,
    CompatibilityCheckResult,
    SchemaVersion,
    FeatureView,
)
from src.schema_registry.registry import (
    register_schema,
    get_schema,
    list_schema_versions,
)
from src.schema_registry.compatibility import check_compatibility
from src.store.feature_registry import (
    register_feature_view,
    get_feature_view,
    list_feature_views,
    deactivate_feature_view,
)
from src.kafka.producer import produce_schema_change

router = APIRouter(prefix="/schemas", tags=["schema-registry"])


@router.post("", response_model=SchemaVersion, status_code=201)
async def create_schema(request: RegisterSchemaRequest):
    """
    Register a new feature view schema.
    If a schema already exists, validates compatibility before incrementing version.
    """
    try:
        schema_ver = await register_schema(request.spec, request.compatibility_mode)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc

    # Persist feature view catalog entry
    await register_feature_view(request.spec, schema_version=schema_ver.version)

    # Notify other consumers about the schema change
    try:
        old_ver = schema_ver.version - 1 if schema_ver.version > 1 else None
        change_type = "create" if schema_ver.version == 1 else "update"
        await produce_schema_change(
            request.spec.name, schema_ver.version, old_ver, change_type
        )
    except Exception:
        pass  # Kafka unavailable shouldn't block schema registration

    return schema_ver


@router.get("/{feature_view}", response_model=SchemaVersion)
async def get_latest_schema(feature_view: str):
    schema = await get_schema(feature_view)
    if not schema:
        raise HTTPException(404, f"No schema found for '{feature_view}'")
    return schema


@router.get("/{feature_view}/{version}", response_model=SchemaVersion)
async def get_schema_version(feature_view: str, version: int):
    schema = await get_schema(feature_view, version)
    if not schema:
        raise HTTPException(404, f"Schema v{version} not found for '{feature_view}'")
    return schema


@router.get("/{feature_view}/versions/all")
async def get_all_versions(feature_view: str):
    return await list_schema_versions(feature_view)


@router.post("/check-compatibility", response_model=CompatibilityCheckResult)
async def check_schema_compatibility(request: CompatibilityCheckRequest):
    """
    Check whether a proposed schema is compatible with the current version
    under the given mode, WITHOUT registering it.
    """
    current = await get_schema(request.feature_view)
    if not current:
        return CompatibilityCheckResult(
            compatible=True, mode="NONE", errors=[], warnings=["No existing schema — first registration is always compatible."]
        )
    return check_compatibility(current.spec, request.new_spec, current.compatibility_mode)


# ------------------------------------------------------------------
# Feature view catalog
# ------------------------------------------------------------------

views_router = APIRouter(prefix="/feature-views", tags=["feature-views"])


@views_router.get("", response_model=list[FeatureView])
async def list_views():
    return await list_feature_views()


@views_router.get("/{name}", response_model=FeatureView)
async def get_view(name: str):
    fv = await get_feature_view(name)
    if not fv:
        raise HTTPException(404, f"Feature view '{name}' not found")
    return fv


@views_router.delete("/{name}")
async def delete_view(name: str):
    ok = await deactivate_feature_view(name)
    if not ok:
        raise HTTPException(404, f"Feature view '{name}' not found")
    return {"deleted": name}
