#!/usr/bin/env python3
"""
YAML validation script for metric view definitions.

This script validates that all YAML files in the view_definitions directory
conform to the expected metric view schema.
"""

import os
import glob
import sys
from typing import List, Dict, Any, Optional
import yaml


def validate_metric_view_yaml(yaml_dict: Dict[str, Any], filename: str) -> List[str]:
    """Validate a single metric view YAML definition.
    
    Args:
        yaml_dict: Parsed YAML as dictionary
        filename: Name of the YAML file being validated
        
    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    
    # Check required top-level fields
    required_fields = ['version', 'source', 'dimensions', 'measures']
    for field in required_fields:
        if field not in yaml_dict:
            errors.append(f"Missing required field: '{field}'")
    
    # Validate version
    if 'version' in yaml_dict:
        if not isinstance(yaml_dict['version'], (int, float, str)):
            errors.append("'version' must be a number or string")
    
    # Validate source
    if 'source' in yaml_dict:
        if not isinstance(yaml_dict['source'], str):
            errors.append("'source' must be a string")
        elif len(yaml_dict['source'].split('.')) < 3:
            errors.append("'source' should be in format 'catalog.schema.table'")
    
    # Validate dimensions
    if 'dimensions' in yaml_dict:
        if not isinstance(yaml_dict['dimensions'], list):
            errors.append("'dimensions' must be a list")
        else:
            for i, dim in enumerate(yaml_dict['dimensions']):
                if not isinstance(dim, dict):
                    errors.append(f"dimensions[{i}] must be an object")
                    continue
                
                if 'name' not in dim:
                    errors.append(f"dimensions[{i}] missing required 'name' field")
                if 'expr' not in dim:
                    errors.append(f"dimensions[{i}] missing required 'expr' field")
    
    # Validate measures  
    if 'measures' in yaml_dict:
        if not isinstance(yaml_dict['measures'], list):
            errors.append("'measures' must be a list")
        else:
            for i, measure in enumerate(yaml_dict['measures']):
                if not isinstance(measure, dict):
                    errors.append(f"measures[{i}] must be an object")
                    continue
                
                if 'name' not in measure:
                    errors.append(f"measures[{i}] missing required 'name' field")
                if 'expr' not in measure:
                    errors.append(f"measures[{i}] missing required 'expr' field")
    
    return errors


def validate_all_yaml_files(yaml_dir: str) -> bool:
    """Validate all YAML files in the given directory.
    
    Args:
        yaml_dir: Directory containing YAML files
        
    Returns:
        True if all files are valid, False otherwise
    """
    if not os.path.exists(yaml_dir):
        print(f"❌ Directory not found: {yaml_dir}")
        return False
    
    yaml_patterns = [
        os.path.join(yaml_dir, "*.yml"),
        os.path.join(yaml_dir, "*.yaml"),
    ]
    
    yaml_files = []
    for pattern in yaml_patterns:
        yaml_files.extend(glob.glob(pattern))
    
    if not yaml_files:
        print(f"❌ No YAML files found in {yaml_dir}")
        return False
    
    print(f"🔍 Validating {len(yaml_files)} YAML files...")
    
    all_valid = True
    
    for yaml_file in yaml_files:
        filename = os.path.basename(yaml_file)
        print(f"\n📄 Validating {filename}...")
        
        try:
            with open(yaml_file, 'r') as f:
                yaml_content = yaml.safe_load(f)
            
            if yaml_content is None:
                print(f"   ❌ File is empty")
                all_valid = False
                continue
            
            errors = validate_metric_view_yaml(yaml_content, filename)
            
            if errors:
                print(f"   ❌ Validation failed:")
                for error in errors:
                    print(f"      • {error}")
                all_valid = False
            else:
                print(f"   ✅ Valid metric view definition")
                
        except yaml.YAMLError as e:
            print(f"   ❌ YAML parsing error: {e}")
            all_valid = False
        except Exception as e:
            print(f"   ❌ Unexpected error: {e}")
            all_valid = False
    
    if all_valid:
        print(f"\n✅ All YAML files are valid!")
    else:
        print(f"\n❌ Some YAML files have validation errors")
    
    return all_valid


def main():
    """Main function."""
    yaml_dir = sys.argv[1] if len(sys.argv) > 1 else "view_definitions"
    
    print(f"🚀 Metric Views YAML Validator")
    print(f"📁 Checking directory: {yaml_dir}")
    
    is_valid = validate_all_yaml_files(yaml_dir)
    
    sys.exit(0 if is_valid else 1)


if __name__ == "__main__":
    main()
