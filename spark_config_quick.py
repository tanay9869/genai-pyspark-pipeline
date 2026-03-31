"""
Quick copy-paste SparkSession configuration for 16GB RAM, 10 CPU cores
Processing 1 million e-commerce orders
"""

from pyspark.sql import SparkSession

# ============ READY-TO-USE CONFIGURATION ============
spark = (
    SparkSession.builder
    .appName("E-Commerce Pipeline")
    # Memory: 6GB driver + 2×4GB executors = 14GB (2GB reserved for OS)
    .config("spark.driver.memory", "6g")
    .config("spark.executor.instances", "2")
    .config("spark.executor.cores", "4")
    .config("spark.executor.memory", "4g")
    # Shuffle: 40 partitions = 10 cores × 4 = balanced for 1M orders
    .config("spark.sql.shuffle.partitions", "40")
    .config("spark.default.parallelism", "40")
    # Adaptive Query Execution: auto-optimization + partition coalescing
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    # Kryo serializer: 10x faster, 2-4x smaller
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryoserializer.buffer.max", "256m")
    # Timeouts: 10 minutes for broadcast joins and network operations
    .config("spark.sql.broadcastTimeout", "600")
    .config("spark.network.timeout", "600s")
    # In-memory caching optimization
    .config("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
    # Cost-based optimizer
    .config("spark.sql.cbo.enabled", "true")
    .getOrCreate()
)

# Verify configuration
print("Spark session created successfully!")
print(f"Version: {spark.version}")
print(f"Master: {spark.sparkContext.master}")
