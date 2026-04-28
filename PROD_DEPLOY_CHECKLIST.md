# Production Deploy Checklist

Use this checklist before merging deployment-impacting changes to `main`.

## CI/CD Readiness

- [ ] CI passes on the PR branch.
- [ ] No skipped critical tests.
- [ ] CD workflow still targets `prod` and references `hastus_prod` environment.

## GitHub Environment Secrets

- [ ] `HASTUS_HOST` is set and points to the correct prod workspace.
- [ ] `HASTUS_CLIENT_ID` is set for the prod Service Principal.
- [ ] `HASTUS_CLIENT_SECRET` is set and valid (not expired/rotated unexpectedly).

## Databricks Access and Permissions

- [ ] Service Principal is present in the prod workspace.
- [ ] Service Principal has permission to deploy to configured bundle `root_path`.
- [ ] Catalog/schema permissions are sufficient for pipeline operations.

## Bundle Configuration Checks

- [ ] `databricks.yml` `prod` target values are correct (`host`, `catalog`, `schema`, `hastus_source`).
- [ ] Resource include scope is still `resources/hastus/*.yml` unless intentionally changed.
- [ ] Compute override for prod is intentional and supported by workspace node types.

## Validation Steps

- [ ] `databricks bundle validate -t prod` succeeds.
- [ ] Critical pipeline code changes are covered by tests.
- [ ] Any migration/DDL risk is reviewed.

## Release and Post-Deploy

- [ ] Merge PR to `main`.
- [ ] Confirm CI finishes successfully on `main`.
- [ ] Confirm CD starts via `workflow_run`.
- [ ] Confirm bundle deploy succeeds.
- [ ] Confirm production pipeline health and expected output.
