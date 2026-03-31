#!/usr/bin/env python3
"""
Analytics runner script for e-commerce data.

This script loads the generated Parquet files and performs comprehensive
analytics using PySpark, displaying results with execution times.
"""

import time
import logging
from pathlib import Path

from src.spark_analytics import ECommerceAnalytics
from src.config import DATA_RAW_DIR, DATA_PROCESSED_DIR

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def format_time(seconds: float) -> str:
    """
    Format time in seconds to a readable string.

    Args:
        seconds: Time in seconds

    Returns:
        Formatted time string
    """
    if seconds < 1:
        return f"{seconds*1000:.2f}ms"
    return f"{seconds:.2f}s"

def main():
    """Main function to run analytics pipeline."""
    try:
        logger.info("Starting E-Commerce Analytics Pipeline")
        analytics = ECommerceAnalytics()

        total_start = time.time()

        # Initialize Spark
        logger.info("Initializing Spark session...")
        init_start = time.time()
        analytics.init_spark()
        init_time = time.time() - init_start
        logger.info(f"Spark session initialized in {format_time(init_time)}")

        # Load data
        logger.info("Loading data from Parquet files...")
        load_start = time.time()
        analytics.load_data(str(DATA_RAW_DIR))
        load_time = time.time() - load_start
        logger.info(f"Data loaded in {format_time(load_time)}")

        print("\n" + "="*70)
        print("CUSTOMER ANALYTICS")
        print("="*70)

        # Top customers analysis
        logger.info("Analyzing top customers...")
        customer_start = time.time()
        customer_analytics = analytics.get_customer_analytics()
        customer_time = time.time() - customer_start

        print(f"\nTop 10 Customers by Spending (computed in {format_time(customer_time)}):")
        print("-" * 70)
        customer_analytics["top_customers"].show(10, truncate=False)

        print(f"\nCustomer Summary (count: {analytics.customers_df.count()}):")
        print("-" * 70)
        customer_analytics["customer_summary"].select(
            "customer_id", "name", "total_orders", "total_spent", "avg_order_value"
        ).show(5, truncate=False)

        print(f"\nSignup Trends by Month:")
        print("-" * 70)
        customer_analytics["signup_trends"].show(truncate=False)

        print("\n" + "="*70)
        print("ORDER ANALYTICS")
        print("="*70)

        # Order analysis
        logger.info("Analyzing orders...")
        order_start = time.time()
        order_analytics = analytics.get_order_analytics()
        order_time = time.time() - order_start

        print(f"\nOrder Status Distribution (computed in {format_time(order_time)}):")
        print("-" * 70)
        order_analytics["status_distribution"].show(truncate=False)

        print(f"\nMonthly Revenue Trends:")
        print("-" * 70)
        order_analytics["revenue_by_date"].orderBy("order_year", "order_month").show(truncate=False)

        print(f"\nOrder Value Distribution:")
        print("-" * 70)
        order_analytics["order_values"].show(truncate=False)

        print("\n" + "="*70)
        print("PRODUCT ANALYTICS")
        print("="*70)

        # Product analysis
        logger.info("Analyzing products...")
        product_start = time.time()
        product_analytics = analytics.get_product_analytics()
        product_time = time.time() - product_start

        print(f"\nSales by Category (computed in {format_time(product_time)}):")
        print("-" * 70)
        product_analytics["category_summary"].show(truncate=False)

        print(f"\nPrice Statistics:")
        print("-" * 70)
        product_analytics["price_stats"].show(truncate=False)

        print(f"\nStock Levels:")
        print("-" * 70)
        product_analytics["stock_levels"].show(truncate=False)

        print("\n" + "="*70)
        print("BUSINESS INSIGHTS")
        print("="*70)

        # Generate business insights
        logger.info("Generating business insights...")
        insights_start = time.time()
        insights = analytics.get_business_insights()
        insights_time = time.time() - insights_start

        print(f"\nKey Metrics (computed in {format_time(insights_time)}):")
        print("-" * 70)
        for key, value in insights.items():
            print(f"{key.replace('_', ' ').title()}: {value:,}")

        total_time = time.time() - total_start

        print("\n" + "="*70)
        print("EXECUTION SUMMARY")
        print("="*70)
        print(f"Spark Initialization:   {format_time(init_time)}")
        print(f"Data Loading:           {format_time(load_time)}")
        print(f"Customer Analytics:     {format_time(customer_time)}")
        print(f"Order Analytics:        {format_time(order_time)}")
        print(f"Product Analytics:      {format_time(product_time)}")
        print(f"Insights Generation:    {format_time(insights_time)}")
        print("-" * 70)
        print(f"Total Execution Time:   {format_time(total_time)}")
        print("="*70)

        logger.info("Analytics pipeline completed successfully!")

    except Exception as e:
        logger.error(f"An error occurred during analytics: {e}")
        raise

    finally:
        logger.info("Stopping Spark session...")
        analytics.stop_spark()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()