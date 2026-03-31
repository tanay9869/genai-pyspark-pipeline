# E-Commerce Data Pipeline

Synthetic e-commerce data generation and PySpark analytics pipeline.

## Overview

This project provides a complete data pipeline for generating synthetic e-commerce data (customers, orders, products) and processing it with PySpark for analytics and insights. It combines synthetic data generation with distributed data processing to create realistic datasets for testing, development, and business analytics.

## Features

- **Synthetic Data Generation**: Generate realistic fake e-commerce data using the Faker library
- **PySpark Analytics**: Perform distributed data processing and analytics on large datasets
- **Modular Architecture**: Clean, extensible codebase with proper testing and logging
- **Type Hints and Docstrings**: Well-documented code with modern Python practices

## Project Structure

```
ecommerce-pipeline/
├── src/
│   ├── __init__.py
│   ├── config.py              # Configuration settings
│   ├── data_generator.py      # Synthetic data generation
│   └── spark_analytics.py     # PySpark analytics
├── data/
│   ├── raw/                   # Raw generated data
│   └── processed/             # Processed analytics results
├── tests/                     # Unit tests
├── notebooks/                 # Jupyter notebooks for exploration
├── main.py                    # Main data generation script
├── run.py                     # Wrapper script for easy execution
├── requirements.txt           # Python dependencies
├── README.md                  # This file
└── .gitignore                 # Git ignore rules
```

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd ecommerce-pipeline
```

2. Create and activate virtual environment:
```bash
python -m venv venv
# On Windows:
venv\Scripts\activate
# On Unix/MacOS:
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Quick Start (Recommended)

Run the pipeline with a single command:
```bash
python3 run.py
```

This wrapper script automatically handles virtual environment activation and runs the data generation pipeline.

### Manual Execution

If you prefer to activate the virtual environment manually:
```bash
# On Windows:
venv\Scripts\activate
python main.py

# On Unix/MacOS:
source venv/bin/activate
python main.py
```

### Generate Synthetic Data

```python
from src.data_generator import ECommerceDataGenerator

generator = ECommerceDataGenerator()
customers_df = generator.generate_customers(1000)
orders_df = generator.generate_orders(5000)
products_df = generator.generate_products(100)
```

### Run Analytics

```python
from src.spark_analytics import ECommerceAnalytics

analytics = ECommerceAnalytics()
analytics.run_full_analytics()
```

### Optimized PySpark Configuration

For processing large datasets on resource-constrained hardware, use the optimized Spark configuration:

```python
from src.spark_config import get_optimized_spark_session, get_session_info

# Create optimized session (16GB RAM, 10 CPU cores, 1M orders)
spark = get_optimized_spark_session()

# View configuration
get_session_info(spark)

# Use it for analytics
from src.spark_analytics import ECommerceAnalytics
analytics = ECommerceAnalytics(spark)
```

Or use the quick version for copy-paste:
```bash
python spark_config_quick.py
```

**Configuration Settings Explained:**
- **spark.driver.memory (6g)**: 40% of 16GB RAM for Spark driver
- **spark.sql.shuffle.partitions (40)**: Optimized for groupBy operations (10 cores × 4)
- **spark.sql.adaptive.enabled**: Auto-optimization of query execution plans
- **spark.serializer (Kryo)**: 10x faster serialization, 2-4x smaller object size
- **spark.sql.adaptive.coalescePartitions.enabled**: Automatic partition coalescing for memory efficiency

For detailed explanation of all 14 configuration parameters, see [src/spark_config.py](src/spark_config.py).

### File Format Benchmarking

Run comprehensive benchmarks comparing CSV, Parquet, and Feather formats:
```bash
python3 run_benchmark.py
```

This script measures file size, read/write times, memory usage, CPU time, and estimated energy consumption for 50,000 rows of data. Results show significant performance improvements with modern columnar formats like Parquet and Feather compared to traditional CSV.

### Configuration

Edit `src/config.py` to modify data generation parameters and analytics settings.

## Data Schema

### Customers
- customer_id: Unique identifier
- name: Customer name
- email: Email address
- address: Full address
- signup_date: Account creation date

### Orders
- order_id: Unique order identifier
- customer_id: Reference to customer
- order_date: Date of order
- total_amount: Order total
- status: Order status (pending, shipped, delivered)

### Products
- product_id: Unique product identifier
- name: Product name
- category: Product category
- price: Product price
- stock_quantity: Available stock

## Development

### Running Tests

```bash
python -m pytest tests/
```

### Code Quality

```bash
# Install dev dependencies
pip install pytest black flake8

# Format code
black src/ tests/

# Lint code
flake8 src/ tests/
```

## Requirements

- Python 3.8+
- Java 8+ (for PySpark)
- PySpark 3.5+

## License

See LICENSE file for details. 
