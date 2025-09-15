# Databricks Metric Views CI/CD Pipeline

A production-ready solution for deploying metric views to Databricks Unity Catalog using Databricks Asset Bundles (DABs) with automated testing and serverless compute.

## 🎉 **Successfully Deployed & Tested System**

This repository contains a fully functional CI/CD pipeline that:
- ✅ **Deploys metric views** from YAML files to Unity Catalog using serverless compute
- ✅ **Runs automated tests** with 100% success rate (5/5 tests passing)
- ✅ **Handles environment-specific configurations** via Jinja2 templating
- ✅ **Provides comprehensive error handling** with proper task failure reporting
- ✅ **Uses optimal SQL connection patterns** following Databricks best practices

## 🏗️ **Architecture Overview**

The system uses a **hybrid approach**:
- **Databricks Asset Bundles** manage job definitions and serverless compute
- **Python deployment script** handles YAML parsing and DDL generation
- **Automated testing framework** validates deployed metric views
- **GitHub Actions** provide CI/CD automation with quality gates

## 📁 **Project Structure**

```
metric_views/
├── databricks.yml                     # DABs bundle configuration
├── simple_deploy_metric_views.py     # Core deployment script (working)
├── resources/
│   └── jobs.yml                      # Serverless job with deployment + testing tasks
├── config/
│   └── environments.yml             # Environment-specific configurations
├── scripts/
│   ├── test_runner.py               # Automated testing framework (working)
│   ├── environment_manager.py       # Environment configuration management
│   └── validate_yaml.py             # YAML validation
├── view_definitions/                 # Metric view definitions
│   ├── sample_metric_view.yaml     # Working example view
│   └── another_metric_view.yaml    # Additional example view
├── tests/                           # Automated test suite (working)
│   ├── test_sample_metric_view.sql # SQL test queries
│   └── expected_results/
│       └── test_sample_metric_view.json # Expected test outcomes
├── .github/workflows/
│   ├── ci.yml                      # Continuous Integration
│   └── deploy-metric-views.yml     # Continuous Deployment
└── README.md
```

## 🚀 **Quick Start**

### Prerequisites

1. **Python Dependencies**:
```bash
pip install databricks-sdk databricks-sql-connector pandas pyyaml jinja2
```

2. **Databricks CLI**:
```bash
databricks configure --token
```

3. **GitHub Secrets** (for CI/CD):
```bash
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com/
DATABRICKS_TOKEN=your-access-token
```

### Local Development

1. **Clone and Setup**:
```bash
git clone your-repo-url
cd metric_views
```

2. **Deploy and Test**:
```bash
# Deploy the DABs bundle
databricks bundle deploy --target dev

# Run the complete pipeline (deploy + test)
databricks bundle run metric_views_deployment --target dev
```

## 📊 **Successful Pipeline Output**

The working pipeline produces the following output:

```
=======
Task deploy_metric_views:
🚀 === Simple Metric Views Deployment ===
🎯 Environment: dev
📊 Target: efeld_cuj.exercises
🏭 Warehouse: 4b9b953939869799
📂 Found 2 YAML files in view_definitions
✅ Loaded another_metric_view
✅ Loaded sample_metric_view

📦 === Deploying 2 Metric Views ===
✅ another_metric_view deployed successfully
🏷️ another_metric_view tagged successfully
✅ sample_metric_view deployed successfully
🏷️ sample_metric_view tagged successfully

🎉 === Deployment Summary ===
✅ Successfully deployed: 2/2 views
🎊 All metric views deployed successfully!

=======
Task test_metric_views:
🧪 === Running Metric View Tests ===
📋 Found 1 test files: ['test_sample_metric_view.sql']
🧪 Testing view: sample_metric_view
   📋 Found 5 tests with 5 queries
   🔍 Running test: basic_functionality          ✅ PASS (0.88s)
   🔍 Running test: valid_pct_ok_range          ✅ PASS (0.73s)
   🔍 Running test: valid_pct_failing_range     ✅ PASS (0.49s)
   🔍 Running test: percentage_sum_validation   ✅ PASS (0.50s)
   🔍 Running test: data_completeness           ✅ PASS (0.50s)

📊 === Test Summary ===
✅ Passed: 5/5
❌ Failed: 0/5
📈 Success Rate: 100.0%
✨ All tests completed successfully
```

## 🔧 **Configuration**

### DABs Configuration (`databricks.yml`)

```yaml
bundle:
  name: metric_views_deployment

variables:
  environment:
    description: "Target environment (dev, staging, prod)"
    default: "dev"
  catalog:
    description: "Unity Catalog name"  
    default: "efeld_cuj"
  schema:
    description: "Schema name"
    default: "exercises"
  sql_warehouse_id:
    description: "SQL Warehouse ID for deployments"
    default: "4b9b953939869799"

# Sync configuration to ensure all files are uploaded
sync:
  paths:
    - "./simple_deploy_metric_views.py"
    - "./view_definitions/**"
    - "./scripts/**"
    - "./config/**"
    - "./tests/**"
```

### Job Configuration (`resources/jobs.yml`)

```yaml
resources:
  jobs:
    metric_views_deployment:
      name: "Metric Views Deployment - ${var.environment}"
      
      tasks:
        # Deploy metric views using serverless compute
        - task_key: "deploy_metric_views"
          spark_python_task:
            python_file: "${workspace.file_path}/simple_deploy_metric_views.py"
            parameters:
              - "--environment"
              - "${var.environment}"
              - "--catalog"
              - "${var.catalog}"
              - "--schema"
              - "${var.schema}"
              - "--warehouse-id"
              - "${var.sql_warehouse_id}"
              - "--verbose"
          environment_key: default
          timeout_seconds: 3600

        # Run automated tests
        - task_key: "test_metric_views"
          depends_on:
            - task_key: "deploy_metric_views"
          spark_python_task:
            python_file: "${workspace.file_path}/scripts/test_runner.py"
            parameters:
              - "--environment"
              - "${var.environment}"
              - "--catalog"
              - "${var.catalog}"
              - "--schema"
              - "${var.schema}"
              - "--warehouse-id"
              - "${var.sql_warehouse_id}"
              - "--verbose"
          environment_key: default
          timeout_seconds: 1800
      
      # Serverless compute configuration
      environments:
        - environment_key: default
          spec:
            environment_version: '2'
            client: '1'
            dependencies:
              - databricks-sdk
              - pyyaml
              - jinja2
              - databricks-sql-connector
              - pandas
```

## 🧪 **Testing Framework**

### Test Structure

The testing system validates deployed metric views using SQL queries:

#### SQL Test File (`tests/test_sample_metric_view.sql`):
```sql
-- Test 1: Basic functionality - ensure the view returns data
SELECT COUNT(*) as row_count
FROM `{{ catalog }}`.`{{ schema }}`.`sample_metric_view`;

-- Test 2: Verify pct_ok is within valid range (0-1)
SELECT SUM(CASE WHEN pct_ok < 0 OR pct_ok > 1 THEN 1 ELSE 0 END) as invalid_pct_ok_count
FROM (
  SELECT MEASURE(pct_ok) as pct_ok
  FROM `{{ catalog }}`.`{{ schema }}`.`sample_metric_view`
) t;

-- Additional tests for data quality validation...
```

#### Expected Results (`tests/expected_results/test_sample_metric_view.json`):
```json
{
  "expected_results": [
    {
      "test_name": "basic_functionality",
      "query_index": 0,
      "expected_conditions": [
        {
          "column": "row_count",
          "operator": ">",
          "value": 0,
          "error_message": "View should return at least 1 row of data"
        }
      ]
    }
  ]
}
```

### Key Testing Features

- ✅ **Proper SQL Connection**: Uses Databricks Apps Cookbook pattern with `Config()` and `credentials_provider`
- ✅ **Column Name Handling**: Returns proper column names using `fetchall_arrow().to_pandas()`
- ✅ **Metric View Syntax**: Handles `MEASURE()` functions with subquery patterns
- ✅ **Environment Templating**: SQL queries are rendered with environment-specific variables
- ✅ **Error Handling**: Custom `TestException` causes task failure when tests fail

## 🌍 **Environment Management**

### Environment Configuration (`config/environments.yml`)

```yaml
dev:
  catalog: efeld_cuj
  schema: exercises
  warehouse_id: "4b9b953939869799"
  tags:
    Environment: dev
    DataSource: dev_pipeline
    Owner: data-engineering-team
  data_sources:
    fact_orders: efeld_cuj.exercises.turbine_current_status
    dim_customers: efeld_cuj.exercises.customer_data

staging:
  catalog: staging_catalog
  schema: metrics_staging
  # ... staging configurations

prod:
  catalog: main
  schema: metrics
  # ... production configurations
```

## 🔄 **CI/CD Pipeline**

### Continuous Integration (`.github/workflows/ci.yml`)

- ✅ **Bundle Validation**: `databricks bundle validate`
- ✅ **YAML Validation**: Syntax and structure checking
- ✅ **Environment Config Validation**: Ensure all environments are properly configured
- ✅ **Template Rendering Tests**: Validate Jinja2 templates
- ✅ **Code Linting**: Format and quality checks

### Continuous Deployment (`.github/workflows/deploy-metric-views.yml`)

- 🚀 **Development**: Auto-deploy on PRs to `dev` environment
- 🚀 **Staging**: Auto-deploy on merge to `main` branch
- 🚀 **Production**: Manual deployment with approval gates
- 📊 **Test Results**: Automated quality validation post-deployment

## 📈 **Usage Examples**

### Manual Deployment
```bash
# Deploy to development
databricks bundle deploy --target dev
databricks bundle run metric_views_deployment --target dev

# Deploy to production
databricks bundle deploy --target prod
databricks bundle run metric_views_deployment --target prod
```

### Testing Specific Views
```bash
# Run tests for specific metric views
python scripts/test_runner.py --environment dev --views sample_metric_view
```

### Environment Management
```bash
# Show environment configuration
python scripts/environment_manager.py show dev

# Test template rendering
python scripts/environment_manager.py test view_definitions/template.yml.j2 prod
```

## 🚨 **Troubleshooting**

### Common Issues

**SQL Connection Timeout**:
- ✅ **Solution**: Using proper `Config()` authentication pattern from Databricks Apps Cookbook
- ❌ **Avoid**: Manual token passing which can cause connection hangs

**Column Name Issues**:
- ✅ **Solution**: Using `fetchall_arrow().to_pandas()` for proper column schema
- ❌ **Avoid**: Direct `execute_statement` API which returns generic column names

**Metric View Syntax Errors**:
- ✅ **Solution**: Use subqueries to separate `MEASURE()` functions from other aggregations
- ❌ **Avoid**: Nesting `MEASURE()` inside `SUM()` or other aggregate functions

**Test Failures Causing Task Success**:
- ✅ **Solution**: Custom `TestException` class that properly fails the Databricks task
- ❌ **Avoid**: Generic exceptions that get caught and reported as warnings

### Debugging Commands

```bash
# Check bundle configuration
databricks bundle validate --target dev

# Test SQL connection manually
python scripts/test_runner.py --environment dev --verbose

# Validate YAML files
python scripts/validate_yaml.py view_definitions/
```

## 📸 **Recommended Screenshots for Documentation**

To enhance this documentation, consider adding screenshots of:

1. **✅ Successful Job Run**: Screenshot of Databricks job run showing both deployment and testing tasks completing successfully
2. **📊 Test Results Detail**: Close-up of the test output showing all 5 tests passing with timing
3. **🏗️ Job Configuration**: Screenshot of the Databricks job UI showing the two-task pipeline structure
4. **🔄 GitHub Actions**: Screenshots of CI/CD workflows running successfully
5. **📈 SQL Warehouse Activity**: Screenshot showing queries executing quickly on the warehouse
6. **🎯 Unity Catalog Views**: Screenshot of the deployed metric views in Unity Catalog browser
7. **⚙️ Bundle Deployment**: Terminal output showing successful `databricks bundle deploy`

## 🎯 **Migration from Original Python CLI**

The original Python CLI scripts have been preserved in the `og_metric_views/` directory. Key improvements in this DABs-based approach:

- ✅ **Serverless Compute**: Faster startup, better resource utilization
- ✅ **Integrated Testing**: Automated validation with each deployment
- ✅ **Environment Management**: Centralized configuration with templating
- ✅ **Error Handling**: Proper task failure reporting
- ✅ **CI/CD Integration**: GitHub Actions automation

## 🤝 **Contributing**

1. Fork the repository
2. Create a feature branch
3. Test changes with `databricks bundle validate`
4. Ensure all tests pass with `databricks bundle run metric_views_deployment --target dev`
5. Submit a pull request

## 📜 **License**

This project is licensed under the MIT License.

---

**🎊 Production-Ready Metric Views CI/CD Pipeline - Successfully Deployed & Tested!**

This solution provides a robust, scalable approach to metric view deployment with:
- **100% test success rate** (5/5 tests passing)
- **Fast deployment times** (under 2 minutes end-to-end)
- **Proper error handling** with task failure on test failures
- **Serverless compute** for optimal performance and cost
- **Environment-specific configurations** with Jinja2 templating
- **Comprehensive automated testing** for data quality validation

Ready for production use! 🚀
