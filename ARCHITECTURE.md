# Hastus Architecture

## Scope

This repository contains a Databricks ETL bundle focused on `hastus_etl` for production deployment, with additional `sdp_advance` resources retained for learning and proof-of-concept work.

## Repository Layout

- `src/hastus_etl/`: production ETL pipeline code.
- `resources/hastus/`: bundle resources deployed by this bundle.
- `resources/sdp_advance/`: non-deployed examples and POC pipeline definitions.
- `test/`: local PySpark unit tests for transformation logic and utilities.
- `.github/workflows/`: CI/CD automation.

## Environment Strategy

- **Dev**
  - Fast iteration.
  - Serverless compute by default for the primary ETL pipeline.
  - Frequent validation and unit testing.
- **Prod**
  - Controlled release path through GitHub Actions.
  - Deploy only after CI succeeds on `main`.
  - Service Principal based authentication.
  - Classic pipeline job cluster override for predictable cost control.

## Deployment Architecture

The Databricks bundle is configured in `databricks.yml`:

- Includes only `resources/hastus/*.yml` for deployment.
- Uses target-specific variables for catalog/schema/source path.
- Uses a Service Principal-owned `root_path` in prod to avoid shared-folder lock and ACL issues during deploy.

## CI/CD Control Points

- **CI Workflow (`.github/workflows/ci.yml`)**
  - Runs tests and coverage on feature branches, PRs to `main`, and `main` push.
  - Provides quality gate before deployment.
- **CD Workflow (`.github/workflows/cd.yml`)**
  - Triggered by `workflow_run` after `CI Workflow` completes.
  - Gated to run only when CI result is `success` and branch is `main`.
  - Uses environment `hastus_prod` secrets:
    - `HASTUS_HOST`
    - `HASTUS_CLIENT_ID`
    - `HASTUS_CLIENT_SECRET`

## Security and Access Model

- No static credentials are stored in source code.
- Production deploy uses OAuth M2M Service Principal credentials from GitHub Environment secrets.
- Fail-fast secret checks in CD prevent ambiguous auth failures.
- Production target permissions are managed in bundle target permissions and workspace ACLs.

## Operational Guidance

- Treat `main` as release-ready.
- Keep resource scope narrow to avoid accidental deployment of POC assets.
- Prefer explicit target overrides over ad-hoc manual changes in workspace UI.
- Keep a rollback path by preserving prior successful commits and bundle state.

## Future Architecture Enhancements

- Add explicit release tags for production promotion.
- Add post-deploy smoke checks in CD (pipeline state and row-level sanity checks).
- Add cost/latency observability dashboard for serverless vs job cluster tuning.
