#!/usr/bin/env python3
"""
Setup and run script for the Basketball Data Pipeline
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path


def setup_environment():
    """Install required packages if not already installed"""
    required_packages = [
        "dagster",
        "dagster-webserver",
        "pandas",
        "requests",
        "beautifulsoup4",
    ]

    print("Checking and installing required packages...")
    for package in required_packages:
        try:
            __import__(package.replace("-", "_"))
            print(f"✓ {package} is already installed")
        except ImportError:
            print(f"Installing {package}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"✓ {package} installed successfully")


def create_directories():
    """Create necessary directories and configuration files"""
    directories = ["data_outputs", "logs", "dagster_home"]

    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"✓ Directory '{directory}' ready")

    # Create configuration files from templates if they don't exist
    config_files = [
        ("dagster.yaml.template", "dagster.yaml"),
        ("workspace.yaml.template", "workspace.yaml"),
    ]

    for template_file, config_file in config_files:
        if not os.path.exists(config_file) and os.path.exists(template_file):
            with open(template_file, "r") as template:
                content = template.read()
            with open(config_file, "w") as config:
                config.write(content)
            print(f"✓ Created '{config_file}' from template")
        elif os.path.exists(config_file):
            print(f"✓ '{config_file}' already exists")
        else:
            print(f"⚠ Template '{template_file}' not found")


def run_dagster_dev():
    """Run Dagster development server"""
    print("\nStarting Dagster development server...")
    print("The web interface will be available at: http://localhost:3000")
    print("Press Ctrl+C to stop the server")

    # Set DAGSTER_HOME environment variable
    env = os.environ.copy()
    env["DAGSTER_HOME"] = os.path.join(os.getcwd(), "dagster_home")

    try:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "dagster",
                "dev",
                "-f",
                "semis_pipeline/pipeline.py",
                "-d",
                "semis_pipeline",  # Set working directory
            ],
            env=env,
        )
    except KeyboardInterrupt:
        print("\nShutting down Dagster server...")


def run_pipeline_once():
    """Run the pipeline once using Dagster CLI"""
    print("\nRunning the basketball pipeline once...")

    # Set DAGSTER_HOME environment variable
    env = os.environ.copy()
    env["DAGSTER_HOME"] = os.path.join(os.getcwd(), "dagster_home")

    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "dagster",
                "job",
                "execute",
                "-f",
                "semis_pipeline/pipeline.py",
                "-d",
                "semis_pipeline",  # Set working directory
                "-j",
                "basketball_pipeline_job",
            ],
            env=env,
        )

        if result.returncode == 0:
            print("✓ Pipeline executed successfully!")
        else:
            print("❌ Pipeline execution failed!")
            return False
    except Exception as e:
        print(f"❌ Error running pipeline: {e}")
        return False

    return True


def check_output():
    """Check if output files were created"""
    output_files = [
        "data_outputs/basketball_reference_players.csv",
        "data_outputs/espn_nba_leaders_pts.csv",
        "data_outputs/merged_players_data.csv",
    ]

    print("\nChecking output files...")
    for file_path in output_files:
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            print(f"✓ {file_path} ({size:,} bytes)")
        else:
            print(f"❌ {file_path} not found")


def main():
    parser = argparse.ArgumentParser(
        description="Basketball Data Pipeline Setup & Runner"
    )
    parser.add_argument(
        "command", choices=["setup", "dev", "run", "check"], help="Command to execute"
    )

    args = parser.parse_args()

    if args.command == "setup":
        print("=== Setting up Basketball Data Pipeline ===")
        setup_environment()
        create_directories()
        print("\n✓ Setup complete! Now you can run:")
        print("  python setup.py dev    # Start development server")
        print("  python setup.py run    # Run pipeline once")

    elif args.command == "dev":
        setup_environment()
        create_directories()
        run_dagster_dev()

    elif args.command == "run":
        setup_environment()
        create_directories()
        success = run_pipeline_once()
        if success:
            check_output()

    elif args.command == "check":
        check_output()


if __name__ == "__main__":
    main()
