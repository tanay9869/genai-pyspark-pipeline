"""
PySpark analytics for e-commerce data.

This module provides analytics and processing capabilities for e-commerce data
using PySpark for distributed computing.
"""

import logging
from typing import Dict, Any, Tuple, Optional
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum, count, avg, max, min, year, month, dayofmonth,
    datediff, current_date, when, desc, asc
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

from .config import SPARK_CONFIG, DATA_RAW_DIR, DATA_PROCESSED_DIR

logger = logging.getLogger(__name__)

class ECommerceAnalytics:
    """
    Analytics engine for e-commerce data using PySpark.

    This class provides methods to load data, perform various analytics,
    and save results for e-commerce datasets.
    """

    def __init__(self):
        """Initialize the analytics engine."""
        self.spark: Optional[SparkSession] = None
        self.customers_df: Optional[DataFrame] = None
        self.orders_df: Optional[DataFrame] = None
        self.products_df: Optional[DataFrame] = None
        logger.info("Initialized ECommerceAnalytics")

    def init_spark(self) -> None:
        """Initialize Spark session with configured settings."""
        if self.spark is None:
            builder = SparkSession.builder \
                .appName(SPARK_CONFIG["app_name"]) \
                .master(SPARK_CONFIG["master"])

            for key, value in SPARK_CONFIG["config"].items():
                builder = builder.config(key, value)

            self.spark = builder.getOrCreate()
            logger.info("Spark session initialized")

    def load_data(self, data_dir: str = str(DATA_RAW_DIR)) -> None:
        """
        Load data from Parquet files into Spark DataFrames.

        Args:
            data_dir: Directory containing the Parquet files
        """
        self.init_spark()

        try:
            self.customers_df = self.spark.read.parquet(f"{data_dir}/customers.parquet")
            self.orders_df = self.spark.read.parquet(f"{data_dir}/orders.parquet")
            self.products_df = self.spark.read.parquet(f"{data_dir}/products.parquet")

            logger.info("Data loaded successfully")
            logger.info(f"Customers: {self.customers_df.count()} records")
            logger.info(f"Orders: {self.orders_df.count()} records")
            logger.info(f"Products: {self.products_df.count()} records")

        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    def get_customer_analytics(self) -> Dict[str, DataFrame]:
        """
        Perform customer-related analytics.

        Returns:
            Dictionary of DataFrames with different customer analytics
        """
        if self.customers_df is None or self.orders_df is None:
            raise ValueError("Data not loaded. Call load_data() first.")

        # Customer order summary
        customer_orders = self.orders_df.groupBy("customer_id") \
            .agg(
                count("order_id").alias("total_orders"),
                sum("total_amount").alias("total_spent"),
                avg("total_amount").alias("avg_order_value"),
                max("order_date").alias("last_order_date")
            )

        customer_summary = self.customers_df.join(
            customer_orders,
            "customer_id",
            "left_outer"
        ).fillna(0)

        # Top customers by spending
        top_customers = customer_summary \
            .orderBy(desc("total_spent")) \
            .limit(10)

        # Customer signup trends
        signup_trends = self.customers_df \
            .withColumn("signup_year", year("signup_date")) \
            .withColumn("signup_month", month("signup_date")) \
            .groupBy("signup_year", "signup_month") \
            .agg(count("customer_id").alias("new_customers")) \
            .orderBy("signup_year", "signup_month")

        return {
            "customer_summary": customer_summary,
            "top_customers": top_customers,
            "signup_trends": signup_trends
        }

    def get_order_analytics(self) -> Dict[str, DataFrame]:
        """
        Perform order-related analytics.

        Returns:
            Dictionary of DataFrames with different order analytics
        """
        if self.orders_df is None:
            raise ValueError("Data not loaded. Call load_data() first.")

        # Order status distribution
        status_distribution = self.orders_df.groupBy("status") \
            .agg(count("order_id").alias("count")) \
            .orderBy(desc("count"))

        # Revenue by date
        revenue_by_date = self.orders_df \
            .withColumn("order_year", year("order_date")) \
            .withColumn("order_month", month("order_date")) \
            .groupBy("order_year", "order_month") \
            .agg(
                sum("total_amount").alias("total_revenue"),
                count("order_id").alias("order_count"),
                avg("total_amount").alias("avg_order_value")
            ) \
            .orderBy("order_year", "order_month")

        # Order value distribution
        order_values = self.orders_df.select("total_amount") \
            .withColumn("value_range",
                       when(col("total_amount") < 50, "Under $50")
                       .when(col("total_amount") < 100, "50-99")
                       .when(col("total_amount") < 200, "100-199")
                       .when(col("total_amount") < 500, "200-499")
                       .otherwise("500+")
                       ) \
            .groupBy("value_range") \
            .agg(count("total_amount").alias("count")) \
            .orderBy("value_range")

        return {
            "status_distribution": status_distribution,
            "revenue_by_date": revenue_by_date,
            "order_values": order_values
        }

    def get_product_analytics(self) -> Dict[str, DataFrame]:
        """
        Perform product-related analytics.

        Returns:
            Dictionary of DataFrames with different product analytics
        """
        if self.products_df is None:
            raise ValueError("Data not loaded. Call load_data() first.")

        # Products by category
        category_summary = self.products_df.groupBy("category") \
            .agg(
                count("product_id").alias("product_count"),
                avg("price").alias("avg_price"),
                sum("stock_quantity").alias("total_stock")
            ) \
            .orderBy(desc("product_count"))

        # Price analysis
        price_stats = self.products_df.select("price") \
            .summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max")

        # Stock levels
        stock_levels = self.products_df \
            .withColumn("stock_status",
                       when(col("stock_quantity") == 0, "Out of Stock")
                       .when(col("stock_quantity") < 10, "Low Stock")
                       .when(col("stock_quantity") < 50, "Medium Stock")
                       .otherwise("Well Stocked")
                       ) \
            .groupBy("stock_status") \
            .agg(count("product_id").alias("count")) \
            .orderBy("stock_status")

        return {
            "category_summary": category_summary,
            "price_stats": price_stats,
            "stock_levels": stock_levels
        }

    def get_business_insights(self) -> Dict[str, Any]:
        """
        Generate key business insights from the data.

        Returns:
            Dictionary containing key metrics and insights
        """
        if not all([self.customers_df, self.orders_df, self.products_df]):
            raise ValueError("Data not loaded. Call load_data() first.")

        # Overall metrics
        total_customers = self.customers_df.count()
        total_orders = self.orders_df.count()
        total_revenue = self.orders_df.agg(sum("total_amount")).collect()[0][0]
        avg_order_value = self.orders_df.agg(avg("total_amount")).collect()[0][0]

        # Customer metrics
        customer_analytics = self.get_customer_analytics()
        avg_orders_per_customer = customer_analytics["customer_summary"] \
            .agg(avg("total_orders")).collect()[0][0]

        # Product metrics
        total_products = self.products_df.count()
        avg_price = self.products_df.agg(avg("price")).collect()[0][0]

        insights = {
            "total_customers": total_customers,
            "total_orders": total_orders,
            "total_revenue": round(total_revenue, 2),
            "avg_order_value": round(avg_order_value, 2),
            "avg_orders_per_customer": round(avg_orders_per_customer, 2),
            "total_products": total_products,
            "avg_product_price": round(avg_price, 2)
        }

        logger.info("Generated business insights")
        return insights

    def save_analytics(self, output_dir: str = str(DATA_PROCESSED_DIR)) -> None:
        """
        Save all analytics results to CSV files.

        Args:
            output_dir: Directory to save results
        """
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Get all analytics
        customer_analytics = self.get_customer_analytics()
        order_analytics = self.get_order_analytics()
        product_analytics = self.get_product_analytics()
        insights = self.get_business_insights()

        # Save DataFrames
        for name, df in customer_analytics.items():
            df.write.csv(f"{output_dir}/customer_{name}.csv", header=True, mode="overwrite")

        for name, df in order_analytics.items():
            df.write.csv(f"{output_dir}/order_{name}.csv", header=True, mode="overwrite")

        for name, df in product_analytics.items():
            df.write.csv(f"{output_dir}/product_{name}.csv", header=True, mode="overwrite")

        # Save insights as JSON
        import json
        with open(f"{output_dir}/business_insights.json", "w") as f:
            json.dump(insights, f, indent=2)

        logger.info(f"Analytics saved to {output_dir}")

    def run_full_analytics(self, data_dir: str = str(DATA_RAW_DIR),
                          output_dir: str = str(DATA_PROCESSED_DIR)) -> Dict[str, Any]:
        """
        Run complete analytics pipeline.

        Args:
            data_dir: Input data directory
            output_dir: Output directory for results

        Returns:
            Business insights dictionary
        """
        logger.info("Starting full analytics pipeline")

        self.load_data(data_dir)

        customer_analytics = self.get_customer_analytics()
        order_analytics = self.get_order_analytics()
        product_analytics = self.get_product_analytics()
        insights = self.get_business_insights()

        self.save_analytics(output_dir)

        logger.info("Analytics pipeline completed")
        return insights

    def stop_spark(self) -> None:
        """Stop the Spark session."""
        if self.spark:
            self.spark.stop()
            self.spark = None
            logger.info("Spark session stopped")