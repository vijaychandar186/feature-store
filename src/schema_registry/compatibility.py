"""
Schema compatibility checker.

Rules mirror the Confluent Schema Registry conventions adapted for
our feature definition schema:

BACKWARD (default, most common):
  A consumer using the NEW schema can read data produced with the OLD schema.
  Safe changes: add new optional feature (has a default), widen numeric type.
  Unsafe: remove a feature, change dtype, make optional → required.

FORWARD:
  A consumer using the OLD schema can read data produced with the NEW schema.
  Safe changes: remove an optional feature, narrow numeric type.
  Unsafe: add required new feature without default.

FULL: both BACKWARD and FORWARD must hold.

NONE: skip all checks.
"""

from src.models.schemas import FeatureViewSpec, CompatibilityMode, CompatibilityCheckResult


def check_compatibility(
    old_spec: FeatureViewSpec,
    new_spec: FeatureViewSpec,
    mode: CompatibilityMode,
) -> CompatibilityCheckResult:
    if mode == "NONE":
        return CompatibilityCheckResult(compatible=True, mode=mode, errors=[], warnings=[])

    errors: list[str] = []
    warnings: list[str] = []

    old_by_name = {f.name: f for f in old_spec.features}
    new_by_name = {f.name: f for f in new_spec.features}

    if mode in ("BACKWARD", "FULL"):
        # New schema must be able to read data written with old schema.
        # → Every old field must still exist in new schema OR have a default in old.
        for name, old_feat in old_by_name.items():
            if name not in new_by_name:
                if old_feat.default is None:
                    errors.append(
                        f"BACKWARD: field '{name}' removed from new schema "
                        f"but had no default — old data cannot be read."
                    )
                else:
                    warnings.append(
                        f"BACKWARD: field '{name}' removed; old data will use default={old_feat.default!r}."
                    )
            else:
                new_feat = new_by_name[name]
                if old_feat.dtype != new_feat.dtype:
                    errors.append(
                        f"BACKWARD: field '{name}' type changed {old_feat.dtype!r} → {new_feat.dtype!r}. "
                        f"Type changes break backward compatibility."
                    )

    if mode in ("FORWARD", "FULL"):
        # Old schema must be able to read data written with new schema.
        # → Every new required field (no default) must exist in old schema.
        for name, new_feat in new_by_name.items():
            if name not in old_by_name:
                if new_feat.default is None:
                    errors.append(
                        f"FORWARD: new required field '{name}' (no default) added to schema — "
                        f"old consumers cannot handle this data."
                    )
                else:
                    warnings.append(
                        f"FORWARD: new optional field '{name}' added; old consumers will ignore it."
                    )
            else:
                old_feat = old_by_name[name]
                if old_feat.dtype != new_feat.dtype:
                    errors.append(
                        f"FORWARD: field '{name}' type changed {old_feat.dtype!r} → {new_feat.dtype!r}."
                    )

    # Entity / timestamp column renaming always breaks compatibility
    if old_spec.entity_column != new_spec.entity_column:
        errors.append(
            f"entity_column renamed '{old_spec.entity_column}' → '{new_spec.entity_column}'. "
            f"This is never compatible."
        )
    if old_spec.timestamp_column != new_spec.timestamp_column:
        errors.append(
            f"timestamp_column renamed '{old_spec.timestamp_column}' → '{new_spec.timestamp_column}'."
        )

    return CompatibilityCheckResult(
        compatible=len(errors) == 0,
        mode=mode,
        errors=errors,
        warnings=warnings,
    )
