#!/usr/bin/env python3
"""
Main script for generating synthetic e-commerce data.

This script generates synthetic customers, products, and orders data,
saves them as Parquet files, and provides timing and file size information.
"""

import time
import os
import logging
from pathlib import Path
from typing import Tuple

from src.data_generator import ECommerceDataGenerator
from src.config import DATA_CONFIG, DATA_RAW_DIR, ensure_directories

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_file_size_mb(file_path: str) -> float:
    """
    Get file size in MB.

    Args:
        file_path: Path to the file

    Returns:
        File size in MB
    """
    size_bytes = os.path.getsize(file_path)
    return size_bytes / (1024 * 1024)

def save_dataframe_as_parquet(df, filename: str, output_dir: Path) -> str:
    """
    Save pandas DataFrame as Parquet file.

    Args:
        df: DataFrame to save
        filename: Name of the file (without extension)
        output_dir: Output directory

    Returns:
        Path to the saved file
    """
    file_path = output_dir / f"{filename}.parquet"
    df.to_parquet(file_path, index=False)
    return str(file_path)

def main():
    """Main function to run the data generation pipeline."""
    try:
        logger.info("Starting synthetic e-commerce data generation")

        # Ensure directories exist
        ensure_directories()

        # Initialize data generator
        generator = ECommerceDataGenerator(seed=42)  # For reproducible results

        total_start_time = time.time()

        # Generate customers
        logger.info("Generating customers...")
        customers_start = time.time()
        customers_df = generator.generate_customers(DATA_CONFIG["num_customers"])
        customers_time = time.time() - customers_start

        # Save customers
        customers_file = save_dataframe_as_parquet(customers_df, "customers", DATA_RAW_DIR)
        customers_size = get_file_size_mb(customers_file)

        logger.info(f"Generated {len(customers_df)} customers in {customers_time:.2f}s")
        logger.info(f"Saved customers to {customers_file} ({customers_size:.2f} MB)")

        # Generate products
        logger.info("Generating products...")
        products_start = time.time()
        products_df = generator.generate_products(DATA_CONFIG["num_products"])
        products_time = time.time() - products_start

        # Save products
        products_file = save_dataframe_as_parquet(products_df, "products", DATA_RAW_DIR)
        products_size = get_file_size_mb(products_file)

        logger.info(f"Generated {len(products_df)} products in {products_time:.2f}s")
        logger.info(f"Saved products to {products_file} ({products_size:.2f} MB)")

        # Generate orders
        logger.info("Generating orders...")
        orders_start = time.time()
        orders_df = generator.generate_orders(
            DATA_CONFIG["num_orders"],
            customers_df=customers_df,
            products_df=products_df
        )
        orders_time = time.time() - orders_start

        # Save orders
        orders_file = save_dataframe_as_parquet(orders_df, "orders", DATA_RAW_DIR)
        orders_size = get_file_size_mb(orders_file)

        logger.info(f"Generated {len(orders_df)} orders in {orders_time:.2f}s")
        logger.info(f"Saved orders to {orders_file} ({orders_size:.2f} MB)")

        # Total time
        total_time = time.time() - total_start_time
        total_size = customers_size + products_size + orders_size

        logger.info("Data generation completed successfully!")
        logger.info(f"Total generation time: {total_time:.2f} seconds")
        logger.info(f"Total data size: {total_size:.2f} MB")

        # Print summary
        print("\n" + "="*50)
        print("SYNTHETIC E-COMMERCE DATA GENERATION SUMMARY")
        print("="*50)
        print(f"Customers: {len(customers_df)} records, {customers_time:.2f}s, {customers_size:.2f} MB")
        print(f"Products:  {len(products_df)} records, {products_time:.2f}s, {products_size:.2f} MB")
        print(f"Orders:    {len(orders_df)} records, {orders_time:.2f}s, {orders_size:.2f} MB")
        print("-"*50)
        print(f"Total:     {len(customers_df) + len(products_df) + len(orders_df)} records, {total_time:.2f}s, {total_size:.2f} MB")
        print("="*50)

    except Exception as e:
        logger.error(f"An error occurred during data generation: {e}")
        raise

if __name__ == "__main__":
    main()