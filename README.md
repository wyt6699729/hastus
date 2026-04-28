# hastus

Databricks Lakeflow / SDP project for ETL pipelines, including:

- `src/`: pipeline source code (bronze/silver/gold transformations)
- `resources/`: Databricks bundle resources (pipelines, jobs, config)
- `test/`: local PySpark unit tests (pytest)

## Quick Onboarding (New Developer)

### 1) Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Java 17 (required for local PySpark)
- Databricks CLI (`databricks`)

### 2) Clone and enter project

```bash
git clone <repo-url>
cd hastus
```

### 3) Create and use local test environment

```bash
uv venv .venv_pyspark
source .venv_pyspark/bin/activate
uv pip install -r requirements-venv-pyspark.txt --python .venv_pyspark/bin/python
```

### 4) Run local tests

```bash
python -m pytest test -q
python -m pytest test --cov=src --cov-report=term-missing -q
```

If Spark startup fails, check Java:

```bash
java -version
echo $JAVA_HOME
```

## Common Development Workflow

### 1) Create a feature branch

```bash
git checkout main
git pull origin main
git checkout -b feature/<your-change>
```

### 2) Implement + test locally

- Add or update code in `src/`
- Add or update tests in `test/`
- Run pytest before opening PR

### 3) Commit and open PR

```bash
git add -A
git commit -m "<message>"
git push -u origin HEAD
```

## Databricks Bundle Usage

### Authenticate

```bash
databricks configure
```

### Validate bundle

```bash
databricks bundle validate
```

### Deploy

```bash
databricks bundle deploy --target dev
databricks bundle deploy --target prod
```

### Run pipeline or job

```bash
databricks bundle run
```

## Pipeline Resources in This Repo

This bundle deploys **only** the resources included from `resources/hastus/` (see `databricks.yml`).

- **Deployed by this bundle**
  - `resources/hastus/hastus_etl.pipeline.yml`
  - `resources/hastus/test_data/` (sample inputs referenced by pipelines/tests)

- **Not deployed by this bundle** (kept for reference / a future SDP bundle)
  - `resources/sdp_advance/dq_pipeline.pipeline.yml`
  - `resources/sdp_advance/multi_flow.pipelines.yml`
  - `resources/sdp_advance/multiflex_pipeline.yml`
  - `resources/sdp_advance/scd_type2.pipeline.yml`

## Helpful Docs

> CI/CD test note: this branch is used to validate GitHub Actions wiring.


- Databricks Bundles: https://docs.databricks.com/dev-tools/bundles/
- Databricks CLI: https://docs.databricks.com/dev-tools/cli/databricks-cli.html
- VS Code / IDE flow: https://docs.databricks.com/dev-tools/vscode-ext.html

## CI/CD (GitHub Actions)

This repository now includes two workflows:

- `.github/workflows/ci.yml`
  - Runs on `feature/**` push and PRs to `main`
  - Sets up Java 17 for local PySpark tests
  - Installs dependencies from `requirements-venv-pyspark.txt`
  - Runs pytest with coverage HTML report and uploads `htmlcov` artifact
- `.github/workflows/cd.yml`
  - Runs on push to `main`
  - Deploys only to `prod`
  - Uses GitHub environment `hastus_prod`
  - Uses Databricks bundle deploy with profile `PROD`

### Required GitHub Secret

For deployment workflow (`cd.yml`), configure:

- GitHub environment: `hastus_prod`
- Secret: `HASTUS_HOST` (Databricks workspace URL, e.g. `https://<workspace-id>.<shard>.gcp.databricks.com`)
- Secret: `HASTUS_CLIENT_ID` (Service Principal OAuth client ID)
- Secret: `HASTUS_CLIENT_SECRET` (Service Principal OAuth client secret)

### Recommended Next Step

If you plan to use this workflow as-is, make sure `prod` target is defined in `databricks.yml` and matches your intended workspace.
