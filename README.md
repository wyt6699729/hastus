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

Examples under `resources/`:

- `hastus_etl.pipeline.yml`
- `dq_pipeline.pipeline.yml`
- `multi_flow.pipelines.yml`
- `multiflex_pipeline.yml`
- `scd_type2.pipeline.yml`

## Helpful Docs

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
  - Deploys to `dev` first, then `prod`
  - Uses Databricks bundle deploy with profiles `DEV` and `PROD`

### Required GitHub Secret

For deployment workflow (`cd.yml`), configure:

- `DATABRICKS_HOST_DEV`: DEV workspace URL
- `DATABRICKS_CLIENT_ID_DEV`: Service Principal OAuth client ID for DEV
- `DATABRICKS_CLIENT_SECRET_DEV`: Service Principal OAuth client secret for DEV
- `DATABRICKS_HOST_PROD`: PROD workspace URL
- `DATABRICKS_CLIENT_ID_PROD`: Service Principal OAuth client ID for PROD
- `DATABRICKS_CLIENT_SECRET_PROD`: Service Principal OAuth client secret for PROD

### Recommended Next Step

If you plan to use this workflow as-is, make sure `dev` and `prod` targets are defined in `databricks.yml` and match your intended workspaces.
