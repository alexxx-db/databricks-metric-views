#!/usr/bin/env python3
"""
Dynamic metric views deployment script.
Automatically discovers and deploys ALL YAML metric view definitions from view_definitions directory.
"""

import os
import sys
import glob
import argparse
from typing import List, Dict, Any, Tuple
import yaml
from databricks.sdk import WorkspaceClient


def load_yaml_files(yaml_directory: str) -> List[Tuple[str, str, Dict[str, Any]]]:
    """Load all YAML files from the directory.
    
    Args:
        yaml_directory: Directory containing YAML files
        
    Returns:
        List of tuples: (view_name, yaml_text, yaml_dict)
    """
    yamls = []
    yaml_patterns = [
        os.path.join(yaml_directory, "*.yml"),
        os.path.join(yaml_directory, "*.yaml"),
    ]
    
    print(f"🔍 Searching for YAML files in: {yaml_directory}")
    for pattern in yaml_patterns:
        files = glob.glob(pattern)
        print(f"Found {len(files)} files matching {pattern}")
        for file in files:
            view_name = os.path.splitext(os.path.basename(file))[0]
            print(f"Loading {file} as view '{view_name}'")
            try:
                with open(file, "r") as f:
                    yaml_text = f.read()
                    yaml_dict = yaml.safe_load(yaml_text)
                    yamls.append((view_name, yaml_text, yaml_dict))
            except Exception as e:
                print(f"⚠️ Error loading {file}: {e}")
                continue
    
    return yamls


def extract_columns(yaml_dict: Dict[str, Any]) -> List[str]:
    """Extract column names from dimensions and measures in the YAML.
    
    Args:
        yaml_dict: Parsed YAML as a dictionary
        
    Returns:
        List of column names
    """
    columns = []
    for dim in yaml_dict.get("dimensions", []):
        columns.append(dim["name"])
    for measure in yaml_dict.get("measures", []):
        columns.append(measure["name"])
    return columns


def generate_metric_view_ddl(view_name: str, yaml_text: str, yaml_dict: Dict[str, Any], catalog: str, schema: str) -> str:
    """Generate CREATE OR REPLACE VIEW DDL for a metric view.
    
    Args:
        view_name: Name of the view
        yaml_text: Raw YAML text
        yaml_dict: Parsed YAML dictionary  
        catalog: Catalog name
        schema: Schema name
        
    Returns:
        DDL statement as string
    """
    columns = extract_columns(yaml_dict)
    column_list = ", ".join(f"`{col}`" for col in columns)
    qualified_view = f"`{catalog}`.`{schema}`.`{view_name}`"
    
    ddl = f"""CREATE OR REPLACE VIEW {qualified_view} (
  {column_list}
) WITH METRICS LANGUAGE YAML AS
$$
{yaml_text}
$$"""
    
    return ddl


def deploy_metric_views():
    """Deploy metric views using command line arguments."""
    
    parser = argparse.ArgumentParser(description="Deploy Databricks Metric Views")
    parser.add_argument("--catalog", default="efeld_cuj", help="Catalog name")
    parser.add_argument("--schema", default="exercises", help="Schema name")  
    parser.add_argument("--warehouse-id", default="4b9b953939869799", help="SQL Warehouse ID")
    parser.add_argument("--yaml-dir", default="view_definitions", help="Directory containing YAML files")
    
    args = parser.parse_args()
    
    catalog = args.catalog
    schema = args.schema
    warehouse_id = args.warehouse_id
    yaml_dir = args.yaml_dir
    
    print(f"🚀 Starting dynamic metric views deployment...")
    print(f"📊 Configuration: {catalog}.{schema} (warehouse: {warehouse_id})")
    print(f"📁 YAML Directory: {yaml_dir}")
    
    # Initialize client
    client = WorkspaceClient()
    
    # Load all YAML files from directory
    yaml_files = load_yaml_files(yaml_dir)
    
    if not yaml_files:
        print(f"❌ No YAML files found in {yaml_dir}")
        return 1
    
    print(f"📊 Found {len(yaml_files)} metric view definitions to deploy")
    
    def execute_sql(sql_statement, description):
        """Execute SQL and handle errors."""
        print(f"🔄 Executing: {description}")
        try:
            response = client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql_statement,
                catalog=catalog,
                schema=schema,
                wait_timeout="30s",
            )
            
            if response.status.state.value == "SUCCEEDED":
                print(f"✅ {description} completed successfully")
                return True
            else:
                print(f"❌ {description} failed: {response.status.error}")
                return False
                
        except Exception as e:
            print(f"❌ {description} failed with exception: {str(e)}")
            return False
    
    # Deploy all metric views dynamically
    success = True
    deployed_views = []
    
    for view_name, yaml_text, yaml_dict in yaml_files:
        print(f"\n--- Deploying {view_name} ---")
        
        try:
            # Generate DDL for this specific view
            ddl = generate_metric_view_ddl(view_name, yaml_text, yaml_dict, catalog, schema)
            
            # Deploy the metric view
            if execute_sql(ddl, f"Deploy {view_name}"):
                deployed_views.append(view_name)
            else:
                success = False
                
        except Exception as e:
            print(f"❌ Failed to deploy {view_name}: {str(e)}")
            success = False
    
    # Apply system tags to all successfully deployed views
    print(f"\n--- Applying System Tags ---")
    for view_name in deployed_views:
        try:
            tag_sql = f"ALTER TABLE `{catalog}`.`{schema}`.`{view_name}` SET TAGS ('system.Certified')"
            execute_sql(tag_sql, f"Apply system tag to {view_name}")
        except Exception as e:
            print(f"⚠️ Warning: Failed to apply tag to {view_name}: {str(e)}")
    
    if success and deployed_views:
        print(f"\n🎉 Successfully deployed {len(deployed_views)} metric views!")
        print("📊 Deployed views:")
        for view_name in deployed_views:
            print(f"   ✅ `{catalog}`.`{schema}`.`{view_name}`")
    elif deployed_views:
        print(f"\n⚠️ Partial success: {len(deployed_views)} views deployed, some may have failed")
        print("📊 Successfully deployed:")
        for view_name in deployed_views:
            print(f"   ✅ `{catalog}`.`{schema}`.`{view_name}`")
        return 1
    else:
        print("❌ No views were successfully deployed")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(deploy_metric_views())
