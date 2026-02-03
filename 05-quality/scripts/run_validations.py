#!/usr/bin/env python3
"""
Data Quality Validation Runner
Run Great Expectations validations from CLI or Airflow.

Usage:
    python run_validations.py                    # Run all checkpoints
    python run_validations.py --checkpoint silver  # Run specific checkpoint
    python run_validations.py --suite silver_jobs  # Run specific suite
    python run_validations.py --docs               # Build data docs
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import FileDataContext


# Configuration
GX_ROOT = Path(__file__).parent.parent / "great_expectations"
DATA_DIR = Path(__file__).parent.parent.parent / "data"


def get_data_context() -> FileDataContext:
    """Get or create Great Expectations data context."""
    return gx.get_context(context_root_dir=str(GX_ROOT))


def run_checkpoint(
    context: FileDataContext,
    checkpoint_name: str,
    batch_request: Optional[dict] = None
) -> dict:
    """Run a specific checkpoint and return results."""
    
    print(f"\n{'='*60}")
    print(f"Running checkpoint: {checkpoint_name}")
    print(f"{'='*60}")
    
    try:
        checkpoint = context.get_checkpoint(name=checkpoint_name)
        
        result = checkpoint.run(
            batch_request=batch_request,
            run_name=f"{checkpoint_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        
        # Parse results
        success = result.success
        statistics = result.run_results
        
        # Print summary
        print(f"\n{'✓' if success else '✗'} Checkpoint: {checkpoint_name}")
        print(f"  Success: {success}")
        
        for run_id, run_result in statistics.items():
            validation_result = run_result.get("validation_result", {})
            stats = validation_result.get("statistics", {})
            print(f"  - Evaluated: {stats.get('evaluated_expectations', 0)} expectations")
            print(f"  - Successful: {stats.get('successful_expectations', 0)}")
            print(f"  - Failed: {stats.get('unsuccessful_expectations', 0)}")
        
        return {
            "checkpoint": checkpoint_name,
            "success": success,
            "statistics": {
                run_id: run_result.get("validation_result", {}).get("statistics", {})
                for run_id, run_result in statistics.items()
            },
            "run_time": datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"✗ Error running checkpoint {checkpoint_name}: {e}")
        return {
            "checkpoint": checkpoint_name,
            "success": False,
            "error": str(e),
            "run_time": datetime.now().isoformat()
        }


def validate_dataframe(
    context: FileDataContext,
    df,
    suite_name: str,
    data_asset_name: str = "runtime_data"
) -> dict:
    """Validate a pandas/spark DataFrame against an expectation suite."""
    
    # Create validator
    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="runtime_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name=data_asset_name,
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": "runtime_batch"}
        ),
        expectation_suite_name=suite_name
    )
    
    # Run validation
    result = validator.validate()
    
    return {
        "suite": suite_name,
        "success": result.success,
        "statistics": result.statistics
    }


def run_all_checkpoints(context: FileDataContext) -> Dict[str, dict]:
    """Run all configured checkpoints."""
    
    # List all checkpoints
    checkpoint_names = context.list_checkpoints()
    
    results = {}
    for name in checkpoint_names:
        results[name] = run_checkpoint(context, name)
    
    return results


def create_silver_checkpoint(context: FileDataContext) -> str:
    """Create checkpoint for silver layer validation."""
    
    checkpoint_config = {
        "name": "silver_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "run_name_template": "silver_%Y%m%d_%H%M%S",
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "local_files",
                    "data_asset_name": "silver_jobs",
                    "options": {}
                },
                "expectation_suite_name": "silver_jobs_suite"
            }
        ],
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction"
                }
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction"
                }
            }
        ]
    }
    
    context.add_checkpoint(**checkpoint_config)
    return "silver_checkpoint"


def build_data_docs(context: FileDataContext, open_browser: bool = False):
    """Build and optionally open data docs."""
    
    print("\nBuilding data docs...")
    context.build_data_docs()
    
    if open_browser:
        context.open_data_docs()
    
    docs_path = GX_ROOT / "uncommitted" / "data_docs" / "local_site" / "index.html"
    print(f"Data docs available at: {docs_path}")


def print_summary(results: Dict[str, dict]):
    """Print validation summary."""
    
    print("\n" + "="*60)
    print("VALIDATION SUMMARY")
    print("="*60)
    
    total = len(results)
    passed = sum(1 for r in results.values() if r.get("success", False))
    failed = total - passed
    
    print(f"\nTotal checkpoints: {total}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    
    if failed > 0:
        print("\nFailed checkpoints:")
        for name, result in results.items():
            if not result.get("success", False):
                print(f"  - {name}")
                if "error" in result:
                    print(f"    Error: {result['error']}")
    
    print("\n" + "="*60)


def main():
    parser = argparse.ArgumentParser(description="Data Quality Validation Runner")
    parser.add_argument("--checkpoint", "-c", help="Run specific checkpoint")
    parser.add_argument("--suite", "-s", help="Run specific expectation suite")
    parser.add_argument("--all", "-a", action="store_true", help="Run all checkpoints")
    parser.add_argument("--docs", "-d", action="store_true", help="Build data docs")
    parser.add_argument("--open", "-o", action="store_true", help="Open data docs in browser")
    parser.add_argument("--output", help="Output results to JSON file")
    
    args = parser.parse_args()
    
    # Get context
    context = get_data_context()
    
    results = {}
    
    if args.checkpoint:
        results[args.checkpoint] = run_checkpoint(context, args.checkpoint)
    elif args.all or (not args.docs and not args.suite):
        results = run_all_checkpoints(context)
    
    if args.docs or args.open:
        build_data_docs(context, open_browser=args.open)
    
    if results:
        print_summary(results)
        
        if args.output:
            with open(args.output, "w") as f:
                json.dump(results, f, indent=2)
            print(f"\nResults saved to: {args.output}")
        
        # Exit with error code if any validation failed
        if not all(r.get("success", False) for r in results.values()):
            sys.exit(1)


if __name__ == "__main__":
    main()
