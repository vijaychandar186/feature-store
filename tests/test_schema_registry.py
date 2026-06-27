"""
Schema registry and compatibility tests.
The compatibility checker runs purely in Python — no DB connection needed.
Schema registration tests require PostgreSQL (mark as integration).
"""

import pytest
from src.models.schemas import FeatureViewSpec, FeatureDefinition, CompatibilityMode
from src.schema_registry.compatibility import check_compatibility


def _spec(features: list[tuple[str, str, object]]) -> FeatureViewSpec:
    """Helper: build spec from [(name, dtype, default)] tuples."""
    return FeatureViewSpec(
        name="test_view",
        features=[
            FeatureDefinition(name=n, dtype=d, default=dflt)
            for n, d, dflt in features
        ],
    )


# ------------------------------------------------------------------
# BACKWARD compatibility
# ------------------------------------------------------------------

def test_backward_add_optional_field_ok():
    old = _spec([("age", "int64", None), ("score", "float64", None)])
    new = _spec([("age", "int64", None), ("score", "float64", None), ("city", "string", "unknown")])
    result = check_compatibility(old, new, "BACKWARD")
    assert result.compatible


def test_backward_add_required_field_ok():
    """Adding a required field is backward-safe — old data just won't have it, but new readers need it."""
    old = _spec([("age", "int64", None)])
    new = _spec([("age", "int64", None), ("name", "string", None)])
    # Adding a field is OK for backward compat (new consumer reads old data; missing field gets None)
    result = check_compatibility(old, new, "BACKWARD")
    assert result.compatible


def test_backward_remove_required_field_fails():
    old = _spec([("age", "int64", None), ("name", "string", None)])
    new = _spec([("age", "int64", None)])
    result = check_compatibility(old, new, "BACKWARD")
    assert not result.compatible
    assert any("name" in e for e in result.errors)


def test_backward_remove_optional_field_warns():
    old = _spec([("age", "int64", None), ("city", "string", "unknown")])
    new = _spec([("age", "int64", None)])
    result = check_compatibility(old, new, "BACKWARD")
    assert result.compatible       # no error — has default
    assert any("city" in w for w in result.warnings)


def test_backward_type_change_fails():
    old = _spec([("age", "int64", None)])
    new = _spec([("age", "string", None)])
    result = check_compatibility(old, new, "BACKWARD")
    assert not result.compatible
    assert any("age" in e for e in result.errors)


# ------------------------------------------------------------------
# FORWARD compatibility
# ------------------------------------------------------------------

def test_forward_add_optional_field_ok():
    old = _spec([("age", "int64", None)])
    new = _spec([("age", "int64", None), ("city", "string", "unknown")])
    result = check_compatibility(old, new, "FORWARD")
    assert result.compatible


def test_forward_add_required_field_fails():
    old = _spec([("age", "int64", None)])
    new = _spec([("age", "int64", None), ("name", "string", None)])
    result = check_compatibility(old, new, "FORWARD")
    assert not result.compatible
    assert any("name" in e for e in result.errors)


def test_forward_remove_field_ok():
    """Removing a field is fine for forward compat — old consumers ignore unknown fields."""
    old = _spec([("age", "int64", None), ("name", "string", None)])
    new = _spec([("age", "int64", None)])
    result = check_compatibility(old, new, "FORWARD")
    assert result.compatible


# ------------------------------------------------------------------
# FULL compatibility
# ------------------------------------------------------------------

def test_full_compatible():
    old = _spec([("age", "int64", None)])
    new = _spec([("age", "int64", None), ("city", "string", "unknown")])
    result = check_compatibility(old, new, "FULL")
    assert result.compatible


def test_full_incompatible_type_change():
    old = _spec([("age", "int64", None)])
    new = _spec([("age", "float64", None)])
    result = check_compatibility(old, new, "FULL")
    assert not result.compatible


# ------------------------------------------------------------------
# NONE
# ------------------------------------------------------------------

def test_none_always_compatible():
    old = _spec([("age", "int64", None)])
    new = _spec([("score", "bool", None)])
    result = check_compatibility(old, new, "NONE")
    assert result.compatible


# ------------------------------------------------------------------
# Entity / timestamp column rename
# ------------------------------------------------------------------

def test_entity_column_rename_fails():
    old = FeatureViewSpec(name="v", entity_column="entity_id", features=[FeatureDefinition(name="f", dtype="int64")])
    new = FeatureViewSpec(name="v", entity_column="user_id", features=[FeatureDefinition(name="f", dtype="int64")])
    result = check_compatibility(old, new, "BACKWARD")
    assert not result.compatible
    assert any("entity_column" in e for e in result.errors)
