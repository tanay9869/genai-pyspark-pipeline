"""
Configuration settings for the E-Commerce Data Pipeline.

This module contains all configuration parameters for data generation,
analytics, and pipeline execution.
"""

import os
from typing import Dict, Any
from pathlib import Path
from datetime import datetime

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

# Data generation settings
DATA_CONFIG = {
    "num_customers": 1000,
    "num_orders": 5000,
    "num_products": 100,
    "start_date": datetime(2020, 1, 1),
    "end_date": datetime(2024, 12, 31)
}

# Spark configuration
SPARK_CONFIG = {
    "app_name": "ECommerceAnalytics",
    "master": "local[*]",
    "config": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.driver.memory": "2g",
        "spark.executor.memory": "2g"
    }
}

# Product categories
PRODUCT_CATEGORIES = [
    "Electronics",
    "Clothing",
    "Home & Garden",
    "Sports & Outdoors",
    "Books",
    "Beauty & Personal Care",
    "Toys & Games",
    "Automotive",
    "Health & Household",
    "Grocery"
]

# Order status options
ORDER_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled"]

# Logging configuration
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": PROJECT_ROOT / "pipeline.log"
}

def get_config() -> Dict[str, Any]:
    """
    Get the complete configuration dictionary.

    Returns:
        Dict containing all configuration settings
    """
    return {
        "project_root": PROJECT_ROOT,
        "data_raw_dir": DATA_RAW_DIR,
        "data_processed_dir": DATA_PROCESSED_DIR,
        "data_config": DATA_CONFIG,
        "spark_config": SPARK_CONFIG,
        "product_categories": PRODUCT_CATEGORIES,
        "order_statuses": ORDER_STATUSES,
        "logging_config": LOGGING_CONFIG
    }

def ensure_directories() -> None:
    """Create necessary directories if they don't exist."""
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)