# Databricks Metric Views Deployment with DABs

A modern Databricks Asset Bundle (DABs) solution for deploying Metric Views from YAML definitions to Unity Catalog. This approach uses serverless compute and automatically discovers all YAML files in the `view_definitions/` directory for dynamic deployment.

## 🚀 Key Features

- **🔄 Fully Dynamic**: Automatically discovers all `.yml` and `.yaml` files in `view_definitions/`
- **🏗️ DABs-Native**: Built on Databricks Asset Bundles for modern deployment patterns
- **⚡ Serverless Compute**: Uses true serverless compute for instant startup and cost efficiency
- **📦 Pipeline-Based**: Runs as parameterized tasks within Databricks jobs
- **🌍 Multi-Environment**: Support for dev, staging, and production deployments
- **🤖 GitHub Actions**: Automated CI/CD pipeline with environment promotion
- **🎯 Smart Deployment**: Generates DDL from actual YAML content and column definitions
- **🏷️ Auto-Tagging**: Automatically applies `system.Certified` tags to deployed views

## 🛠️ Installation & Setup

### Prerequisites
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) installed and configured
- Access to a Databricks workspace with Unity Catalog enabled
- Appropriate permissions for the target catalog and SQL warehouse

### Setup Databricks CLI Profile
```bash
databricks configure --profile e2-field-eng-west
# Enter your workspace URL and token when prompted
```

### Validate Configuration
```bash
# Test your connection
databricks workspace list --profile e2-field-eng-west

# Validate the DABs bundle
databricks bundle validate --target dev
```

## 🚀 Usage

### Quick Start (Local Development)
```bash
# Deploy to development environment
./scripts/local_deploy.sh dev

# Deploy to staging environment  
./scripts/local_deploy.sh staging

# Deploy to production environment
./scripts/local_deploy.sh prod
```

### Manual DABs Commands
```bash
# Deploy bundle to specific environment
databricks bundle deploy --target dev

# Run the metric views deployment job
databricks bundle run --target dev metric_views_deployment

# Check deployment status
databricks jobs list --profile e2-field-eng-west
```

### GitHub Actions (Automated)
The workflow automatically:
- **Pull Request**: Deploys to `dev` environment for testing
- **Main Branch**: Deploys to `staging` environment
- **Manual Trigger**: Allows deployment to `prod` environment

Required GitHub Secrets:
- `DATABRICKS_HOST`: Your workspace URL
- `DATABRICKS_TOKEN`: Personal access token or service principal token

## YAML Format

Metric views are defined in YAML files with the following structure:

```yaml
version: 0.1

source: your.catalog.schema.table

dimensions:
  - name: region
    expr: region

  - name: product_category
    expr: product_category

measures:
  - name: total_sales
    expr: sum(sale_amount)

  - name: avg_order_value
    expr: avg(sale_amount)
```

The DABs pipeline will:
1. Automatically discover all YAML files in `view_definitions/`
2. Extract column names from `dimensions` and `measures`
3. Generate appropriate `CREATE OR REPLACE VIEW` DDL with metrics language
4. Execute the SQL against your Databricks workspace
5. Apply the `system.Certified` tag to deployed views

## 📁 Project Structure

```
metric_views/
├── databricks.yml              # Main DABs bundle configuration
├── resources/                  # DABs resource definitions
│   └── jobs.yml               # Job and task definitions with serverless compute
├── deploy_metric_views.py     # Dynamic deployment script (discovers all YAMLs)
├── view_definitions/          # Metric view YAML definitions (auto-discovered)
│   ├── sample_metric_view.yaml
│   └── another_metric_view.yaml
├── scripts/                   # Utility scripts
│   ├── local_deploy.sh       # Local deployment helper
│   └── validate_yaml.py      # YAML validation script
└── .github/workflows/         # CI/CD workflows
    └── deploy-metric-views.yml
```

## 🔧 Development & Validation

### YAML Validation
```bash
# Validate all YAML files
python scripts/validate_yaml.py

# Validate specific directory
python scripts/validate_yaml.py custom_yaml_dir/
```

### Adding New Metric Views
1. **Create YAML file**: Drop any new `.yml` or `.yaml` file in `view_definitions/`
2. **Automatic Discovery**: The deployment script will automatically find and deploy it
3. **Validate syntax**: `python scripts/validate_yaml.py` (optional)
4. **Test locally**: `./scripts/local_deploy.sh dev`
5. **Deploy via PR**: Create a pull request → automatic deployment to dev → merge → automatic staging deployment

**No code changes needed!** Just add/modify/delete YAML files and the system handles the rest.

### Environment Configuration
The DABs configuration supports multiple environments with different settings:

| Environment | Catalog     | Schema    | Usage           |
|-------------|-------------|-----------|-----------------|
| `dev`       | efeld_cuj   | exercises | Development     |
| `staging`   | efeld_cuj   | staging   | Pre-production  |
| `prod`      | efeld_cuj   | prod      | Production      |

### ⚡ Optimized Compute Configuration

The deployment is configured to automatically use the **most efficient compute available** in your workspace:

- **🏃‍♂️ Serverless-Ready**: Automatically uses Databricks Serverless Compute when enabled in your workspace
- **🔄 Adaptive**: Falls back to optimized traditional compute if serverless isn't available
- **🚀 Fast Startup**: Minimizes cold start time with efficient job configurations
- **💰 Cost Efficient**: Uses compute only when needed, with automatic scaling
- **🛠️ Zero Configuration**: No cluster sizing or management decisions required

**Serverless Benefits** (when available):
- **Instant Startup**: Jobs start in seconds, not minutes
- **Pay-per-Use**: Only pay for actual execution time
- **Auto-Scaling**: Scales automatically based on workload
- **Enhanced Security**: Built-in isolation and security controls

This approach is ideal for metric view deployments which are typically:
- **Intermittent**: Run on-demand or scheduled, not continuously
- **Variable Load**: Processing time depends on number of YAML files  
- **Short Duration**: Most deployments complete within minutes

## 🚨 Troubleshooting

### Common Issues

**Bundle validation fails:**
```bash
# Check bundle syntax
databricks bundle validate --target dev

# Check detailed configuration
databricks bundle validate --target dev --verbose
```

**Job execution fails:**
1. Check job status in Databricks UI
2. Review job run logs for specific error messages
3. Verify SQL warehouse permissions
4. Ensure catalog/schema access permissions

**YAML files not processed:**
- Ensure files are in `view_definitions/` directory
- Check file extensions (`.yml` or `.yaml`)
- Run validation script: `python scripts/validate_yaml.py`

### Migration from Legacy Python CLI

If migrating from the previous Python-based approach:

1. **YAML files**: No changes needed - same format
2. **Authentication**: Uses Databricks CLI profiles (same as before)  
3. **Deployment**: Use `./scripts/local_deploy.sh` instead of `uv run python -m ...`
4. **CI/CD**: GitHub Actions workflow handles deployment automatically

## 🔄 Migration Guide

### From Python CLI to DABs

**Before (Python CLI):**
```bash
uv run python -m metric_views_processor.cli \
  --warehouse-id 4b9b953939869799 \
  --catalog efeld_cuj \
  --schema exercises \
  --yaml-dir view_definitions \
  --profile e2-field-eng-west
```

**After (DABs):**
```bash
./scripts/local_deploy.sh dev
```

The DABs approach provides:
- ✅ **Serverless compute** for faster startup times and automatic scaling
- ✅ **Cost efficiency** with pay-per-use serverless pricing
- ✅ Better scalability and parameterization
- ✅ Integration with Databricks job scheduling  
- ✅ Multi-environment support out of the box
- ✅ Better observability and monitoring
- ✅ Native Databricks integration