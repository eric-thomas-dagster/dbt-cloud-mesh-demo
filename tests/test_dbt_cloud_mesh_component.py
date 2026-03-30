"""Tests for DbtCloudMeshComponent's cross-project lineage preservation.

These tests verify that:
1. Excluded package assets are NOT created in the downstream component
2. Included downstream assets still have deps on upstream excluded package assets
3. No duplicate ownership behavior is reintroduced
"""

import json
from pathlib import Path
from unittest.mock import patch

import dagster as dg
from dagster_dbt import DagsterDbtTranslator
from dagster_dbt.asset_utils import get_upstream_unique_ids

from dbt_cloud_mesh_demo.components.dbt_cloud_mesh_component import (
    DbtCloudMeshComponent,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────

GOLD_MANIFEST_PATH = (
    Path(__file__).parent.parent / "dbt_projects" / "gold_project" / "target" / "manifest.json"
)

# If the gold_project manifest doesn't exist in the cloud demo, fall back to
# the dbt-mesh-demo's manifest
FALLBACK_MANIFEST_PATH = (
    Path(__file__).parent.parent.parent
    / "dbt-mesh-demo"
    / "dbt_projects"
    / "gold_project"
    / "target"
    / "manifest.json"
)


def _load_manifest() -> dict:
    for p in [GOLD_MANIFEST_PATH, FALLBACK_MANIFEST_PATH]:
        if p.exists():
            return json.loads(p.read_text())
    raise FileNotFoundError(
        "No gold_project manifest.json found. "
        "Run `dbt build` in the gold_project first."
    )


MANIFEST = _load_manifest()
TRANSLATOR = DagsterDbtTranslator()


def _gold_node_ids() -> set[str]:
    return {
        uid
        for uid, node in MANIFEST["nodes"].items()
        if node.get("package_name") == "gold_project"
        and node.get("resource_type") == "model"
    }


def _silver_node_ids() -> set[str]:
    return {
        uid
        for uid, node in MANIFEST["nodes"].items()
        if node.get("package_name") == "silver_project"
    }


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestExcludedPackageAssetsNotCreated:
    """Excluded package assets must NOT appear as assets in the downstream component."""

    def test_restore_excluded_deps_does_not_create_silver_specs(self):
        """_restore_excluded_deps should not add new specs for excluded nodes."""
        component = DbtCloudMeshComponent(
            workspace={"account_id": 1, "token": "x", "project_id": 1, "environment_id": 1},
            external_packages={"silver_project": {"key_prefix": ["silver_project"]}},
            exclude="package:silver_project",
        )

        # Start with only gold specs (simulating what exclude produces)
        gold_specs = []
        for uid in _gold_node_ids():
            node = MANIFEST["nodes"][uid]
            key = TRANSLATOR.get_asset_key(node)
            gold_specs.append(dg.AssetSpec(key=key))

        restored = component._restore_excluded_deps(gold_specs, MANIFEST)

        # Same number of specs - no new assets created
        assert len(restored) == len(gold_specs)

        # All spec keys are gold keys only
        gold_keys = {str(s.key) for s in gold_specs}
        restored_keys = {str(s.key) for s in restored}
        assert restored_keys == gold_keys


class TestLineagePreserved:
    """Downstream assets must retain dependency edges to excluded upstream assets."""

    def test_gold_assets_depend_on_silver_keys(self):
        """After _restore_excluded_deps, gold specs must have deps on silver keys."""
        component = DbtCloudMeshComponent(
            workspace={"account_id": 1, "token": "x", "project_id": 1, "environment_id": 1},
            external_packages={"silver_project": {"key_prefix": ["silver_project"]}},
            exclude="package:silver_project",
        )

        # Build gold-only specs with NO deps (worst case: exclude dropped them)
        gold_specs = []
        for uid in _gold_node_ids():
            node = MANIFEST["nodes"][uid]
            key = TRANSLATOR.get_asset_key(node)
            gold_specs.append(dg.AssetSpec(key=key))

        restored = component._restore_excluded_deps(gold_specs, MANIFEST)

        # Every gold model that depends on silver in the manifest should
        # have those deps restored
        for uid in _gold_node_ids():
            node = MANIFEST["nodes"][uid]
            upstream_ids = get_upstream_unique_ids(MANIFEST, node)
            silver_upstream_ids = upstream_ids & _silver_node_ids()

            if not silver_upstream_ids:
                continue

            gold_key = TRANSLATOR.get_asset_key(node)
            restored_spec = next(s for s in restored if s.key == gold_key)
            dep_keys = {str(dep.asset_key) for dep in restored_spec.deps}

            for silver_uid in silver_upstream_ids:
                silver_node = MANIFEST["nodes"][silver_uid]
                expected_key = TRANSLATOR.get_asset_key(silver_node)
                assert str(expected_key) in dep_keys, (
                    f"{gold_key} should depend on {expected_key} "
                    f"(from {silver_uid}), but deps are {dep_keys}"
                )

    def test_gold_assets_with_existing_deps_are_not_duplicated(self):
        """If deps already exist (exclude preserved them), they should not be added twice."""
        component = DbtCloudMeshComponent(
            workspace={"account_id": 1, "token": "x", "project_id": 1, "environment_id": 1},
            external_packages={"silver_project": {}},
            exclude="package:silver_project",
        )

        # Build gold specs WITH correct deps already present
        gold_specs = []
        for uid in _gold_node_ids():
            node = MANIFEST["nodes"][uid]
            key = TRANSLATOR.get_asset_key(node)
            upstream_ids = get_upstream_unique_ids(MANIFEST, node)
            deps = [
                dg.AssetDep(asset=TRANSLATOR.get_asset_key(MANIFEST["nodes"][u]))
                for u in upstream_ids
                if u in MANIFEST["nodes"]
            ]
            gold_specs.append(dg.AssetSpec(key=key, deps=deps))

        restored = component._restore_excluded_deps(gold_specs, MANIFEST)

        # Same deps count - no duplicates added
        for original, patched in zip(
            sorted(gold_specs, key=lambda s: str(s.key)),
            sorted(restored, key=lambda s: str(s.key)),
        ):
            assert len(patched.deps) == len(original.deps), (
                f"{original.key}: expected {len(original.deps)} deps, "
                f"got {len(patched.deps)}"
            )


class TestNoDuplicateOwnership:
    """External packages must not reintroduce duplicate asset ownership."""

    def test_no_silver_asset_specs_in_output(self):
        """The component should never output AssetSpecs for silver models."""
        component = DbtCloudMeshComponent(
            workspace={"account_id": 1, "token": "x", "project_id": 1, "environment_id": 1},
            external_packages={"silver_project": {}},
            exclude="package:silver_project",
        )

        gold_specs = []
        for uid in _gold_node_ids():
            node = MANIFEST["nodes"][uid]
            key = TRANSLATOR.get_asset_key(node)
            gold_specs.append(dg.AssetSpec(key=key))

        restored = component._restore_excluded_deps(gold_specs, MANIFEST)

        silver_keys = set()
        for uid in _silver_node_ids():
            node = MANIFEST["nodes"][uid]
            silver_keys.add(str(TRANSLATOR.get_asset_key(node)))

        for spec in restored:
            assert str(spec.key) not in silver_keys, (
                f"Silver asset {spec.key} should not be in gold component output"
            )

    def test_manifest_upstream_deps_are_complete(self):
        """Every manifest-level cross-project dep must be represented."""
        component = DbtCloudMeshComponent(
            workspace={"account_id": 1, "token": "x", "project_id": 1, "environment_id": 1},
            external_packages={"silver_project": {}},
            exclude="package:silver_project",
        )

        gold_specs = [
            dg.AssetSpec(key=TRANSLATOR.get_asset_key(MANIFEST["nodes"][uid]))
            for uid in _gold_node_ids()
        ]
        restored = component._restore_excluded_deps(gold_specs, MANIFEST)

        # Count total cross-project deps in manifest
        expected_cross_deps = 0
        for uid in _gold_node_ids():
            node = MANIFEST["nodes"][uid]
            upstream_ids = get_upstream_unique_ids(MANIFEST, node)
            expected_cross_deps += len(upstream_ids & _silver_node_ids())

        # Count total cross-project deps in restored specs
        silver_keys = {
            str(TRANSLATOR.get_asset_key(MANIFEST["nodes"][uid]))
            for uid in _silver_node_ids()
            if uid in MANIFEST["nodes"]
        }
        actual_cross_deps = sum(
            1
            for spec in restored
            for dep in spec.deps
            if str(dep.asset_key) in silver_keys
        )

        assert actual_cross_deps == expected_cross_deps, (
            f"Expected {expected_cross_deps} cross-project deps, "
            f"got {actual_cross_deps}"
        )
