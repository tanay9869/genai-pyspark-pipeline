#!/usr/bin/env python3
"""
File Format Benchmarking Script

Benchmarks various file formats (CSV, XLSX, Parquet, ORC, Feather) with hardware metrics
including file size, read/write times, memory usage, CPU time, and estimated energy consumption.
"""

import time
import tracemalloc
import os
import psutil
from pathlib import Path
from typing import Dict, List, Tuple, Any
import pandas as pd
from faker import Faker
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.orc as orc
import pyarrow.feather as feather
import fastparquet
from datetime import datetime, timedelta

# For XLSX support
try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

class FileFormatBenchmark:
    """Benchmark class for file format performance testing."""

    def __init__(self, num_rows: int = 50000):  # Reduced from 500,000 for faster execution
        """Initialize the benchmark with specified number of rows."""
        self.num_rows = num_rows
        self.fake = Faker()
        self.results = {}
        self.temp_dir = Path("temp_benchmark")
        self.temp_dir.mkdir(exist_ok=True)

        # Set random seed for reproducible results
        Faker.seed(42)

    def generate_data(self) -> pd.DataFrame:
        """Generate synthetic e-commerce data."""
        print(f"Generating {self.num_rows:,} rows of synthetic data...")

        start_time = time.time()
        data = []

        categories = ["Electronics", "Clothing", "Home", "Sports", "Books", "Beauty", "Toys", "Automotive"]
        base_date = datetime(2020, 1, 1)

        for i in range(self.num_rows):
            record = {
                'id': i + 1,
                'name': self.fake.name(),
                'email': self.fake.email(),
                'amount': round(self.fake.random.uniform(10.0, 1000.0), 2),
                'date': base_date + timedelta(days=self.fake.random_int(0, 1825)),  # ~5 years
                'category': self.fake.random_element(categories)
            }
            data.append(record)

        df = pd.DataFrame(data)
        generation_time = time.time() - start_time
        print(".2f")

        return df

    def measure_operation(self, operation_func, *args, **kwargs) -> Tuple[float, float, float]:
        """
        Measure CPU time, wall time, and peak memory for an operation.

        Returns:
            Tuple of (cpu_time, wall_time, peak_memory_mb)
        """
        tracemalloc.start()
        start_cpu = time.process_time()
        start_wall = time.time()

        result = operation_func(*args, **kwargs)

        end_cpu = time.process_time()
        end_wall = time.time()
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        cpu_time = end_cpu - start_cpu
        wall_time = end_wall - start_wall
        peak_memory_mb = peak / (1024 * 1024)

        return cpu_time, wall_time, peak_memory_mb, result

    def benchmark_csv(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Benchmark CSV format."""
        file_path = self.temp_dir / "benchmark.csv"

        # Write benchmark
        cpu_write, wall_write, mem_write, _ = self.measure_operation(
            lambda: df.to_csv(file_path, index=False)
        )

        # Get file size
        file_size_mb = file_path.stat().st_size / (1024 * 1024)

        # Read benchmark
        cpu_read, wall_read, mem_read, _ = self.measure_operation(
            lambda: pd.read_csv(file_path)
        )

        # Cleanup
        file_path.unlink(missing_ok=True)

        return {
            'format': 'CSV',
            'file_size_mb': file_size_mb,
            'write_time_sec': wall_write,
            'read_time_sec': wall_read,
            'write_cpu_time': cpu_write,
            'read_cpu_time': cpu_read,
            'write_peak_memory_mb': mem_write,
            'read_peak_memory_mb': mem_read
        }

    def benchmark_xlsx(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Benchmark XLSX format."""
        if not HAS_OPENPYXL:
            return {
                'format': 'XLSX',
                'file_size_mb': float('nan'),
                'write_time_sec': float('nan'),
                'read_time_sec': float('nan'),
                'write_cpu_time': float('nan'),
                'read_cpu_time': float('nan'),
                'write_peak_memory_mb': float('nan'),
                'read_peak_memory_mb': float('nan'),
                'error': 'openpyxl not installed'
            }

        file_path = self.temp_dir / "benchmark.xlsx"

        # Write benchmark
        cpu_write, wall_write, mem_write, _ = self.measure_operation(
            lambda: df.to_excel(file_path, index=False, engine='openpyxl')
        )

        # Get file size
        file_size_mb = file_path.stat().st_size / (1024 * 1024)

        # Read benchmark
        cpu_read, wall_read, mem_read, _ = self.measure_operation(
            lambda: pd.read_excel(file_path, engine='openpyxl')
        )

        # Cleanup
        file_path.unlink(missing_ok=True)

        return {
            'format': 'XLSX',
            'file_size_mb': file_size_mb,
            'write_time_sec': wall_write,
            'read_time_sec': wall_read,
            'write_cpu_time': cpu_write,
            'read_cpu_time': cpu_read,
            'write_peak_memory_mb': mem_write,
            'read_peak_memory_mb': mem_read
        }

    def benchmark_parquet_pyarrow(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Benchmark Parquet format using PyArrow."""
        file_path = self.temp_dir / "benchmark_pyarrow.parquet"

        # Write benchmark
        table = pa.Table.from_pandas(df)
        cpu_write, wall_write, mem_write, _ = self.measure_operation(
            lambda: pq.write_table(table, file_path)
        )

        # Get file size
        file_size_mb = file_path.stat().st_size / (1024 * 1024)

        # Read benchmark
        cpu_read, wall_read, mem_read, _ = self.measure_operation(
            lambda: pq.read_table(file_path).to_pandas()
        )

        # Cleanup
        file_path.unlink(missing_ok=True)

        return {
            'format': 'Parquet (PyArrow)',
            'file_size_mb': file_size_mb,
            'write_time_sec': wall_write,
            'read_time_sec': wall_read,
            'write_cpu_time': cpu_write,
            'read_cpu_time': cpu_read,
            'write_peak_memory_mb': mem_write,
            'read_peak_memory_mb': mem_read
        }

    def benchmark_parquet_fastparquet(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Benchmark Parquet format using fastparquet."""
        file_path = self.temp_dir / "benchmark_fastparquet.parquet"

        # Write benchmark
        cpu_write, wall_write, mem_write, _ = self.measure_operation(
            lambda: df.to_parquet(file_path, engine='fastparquet', index=False)
        )

        # Get file size
        file_size_mb = file_path.stat().st_size / (1024 * 1024)

        # Read benchmark
        cpu_read, wall_read, mem_read, _ = self.measure_operation(
            lambda: pd.read_parquet(file_path, engine='fastparquet')
        )

        # Cleanup
        file_path.unlink(missing_ok=True)

        return {
            'format': 'Parquet (fastparquet)',
            'file_size_mb': file_size_mb,
            'write_time_sec': wall_write,
            'read_time_sec': wall_read,
            'write_cpu_time': cpu_write,
            'read_cpu_time': cpu_read,
            'write_peak_memory_mb': mem_write,
            'read_peak_memory_mb': mem_read
        }

    def benchmark_orc(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Benchmark ORC format."""
        file_path = self.temp_dir / "benchmark.orc"

        # Write benchmark
        table = pa.Table.from_pandas(df)
        cpu_write, wall_write, mem_write, _ = self.measure_operation(
            lambda: orc.write_table(table, file_path)
        )

        # Get file size
        file_size_mb = file_path.stat().st_size / (1024 * 1024)

        # Read benchmark
        cpu_read, wall_read, mem_read, _ = self.measure_operation(
            lambda: orc.read_table(file_path).to_pandas()
        )

        # Cleanup
        file_path.unlink(missing_ok=True)

        return {
            'format': 'ORC',
            'file_size_mb': file_size_mb,
            'write_time_sec': wall_write,
            'read_time_sec': wall_read,
            'write_cpu_time': cpu_write,
            'read_cpu_time': cpu_read,
            'write_peak_memory_mb': mem_write,
            'read_peak_memory_mb': mem_read
        }

    def benchmark_feather(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Benchmark Feather format."""
        file_path = self.temp_dir / "benchmark.feather"

        # Write benchmark
        cpu_write, wall_write, mem_write, _ = self.measure_operation(
            lambda: feather.write_feather(df, file_path)
        )

        # Get file size
        file_size_mb = file_path.stat().st_size / (1024 * 1024)

        # Read benchmark
        cpu_read, wall_read, mem_read, _ = self.measure_operation(
            lambda: feather.read_feather(file_path)
        )

        # Cleanup
        file_path.unlink(missing_ok=True)

        return {
            'format': 'Feather',
            'file_size_mb': file_size_mb,
            'write_time_sec': wall_write,
            'read_time_sec': wall_read,
            'write_cpu_time': cpu_write,
            'read_cpu_time': cpu_read,
            'write_peak_memory_mb': mem_write,
            'read_peak_memory_mb': mem_read
        }

    def calculate_energy_consumption(self, cpu_time_sec: float, tdp_watts: float = 65) -> float:
        """Calculate estimated energy consumption in Wh."""
        return (cpu_time_sec * tdp_watts) / 3600

    def run_benchmarks(self) -> pd.DataFrame:
        """Run all benchmarks and return results."""
        print("Starting file format benchmarks...")
        print("=" * 80)

        # Generate data
        df = self.generate_data()

        # Run benchmarks
        import platform
        is_windows = platform.system() == 'Windows'

        benchmarks = [
            ('CSV', self.benchmark_csv),
            # ('XLSX', self.benchmark_xlsx),  # Skip XLSX as it's very slow for large datasets
            ('Parquet (PyArrow)', self.benchmark_parquet_pyarrow),
            ('Parquet (fastparquet)', self.benchmark_parquet_fastparquet),
            ('Feather', self.benchmark_feather),
        ]

        # Add ORC only if not on Windows (due to Hadoop configuration issues)
        if not is_windows:
            benchmarks.append(('ORC', self.benchmark_orc))

        results = []
        for format_name, benchmark_func in benchmarks:
            print(f"Benchmarking {format_name}...")
            try:
                result = benchmark_func(df)
                results.append(result)
                print(".2f")
            except Exception as e:
                print(f"  Error benchmarking {format_name}: {e}")
                results.append({
                    'format': format_name,
                    'file_size_mb': float('nan'),
                    'write_time_sec': float('nan'),
                    'read_time_sec': float('nan'),
                    'write_cpu_time': float('nan'),
                    'read_cpu_time': float('nan'),
                    'write_peak_memory_mb': float('nan'),
                    'read_peak_memory_mb': float('nan'),
                    'error': str(e)
                })

        # Convert to DataFrame
        results_df = pd.DataFrame(results)

        # Calculate additional metrics
        results_df['total_cpu_time'] = results_df['write_cpu_time'] + results_df['read_cpu_time']
        results_df['total_time_sec'] = results_df['write_time_sec'] + results_df['read_time_sec']
        results_df['energy_consumption_wh'] = results_df['total_cpu_time'].apply(
            lambda x: self.calculate_energy_consumption(x) if not pd.isna(x) else float('nan')
        )

        # Calculate percentage savings vs CSV
        csv_mask = results_df['format'] == 'CSV'
        if csv_mask.any():
            csv_values = results_df[csv_mask].iloc[0]
            for col in ['file_size_mb', 'write_time_sec', 'read_time_sec', 'total_time_sec',
                       'write_peak_memory_mb', 'read_peak_memory_mb', 'energy_consumption_wh']:
                if not pd.isna(csv_values[col]) and csv_values[col] != 0:
                    results_df[f'{col}_savings_pct'] = (
                        (csv_values[col] - results_df[col]) / csv_values[col] * 100
                    ).round(1)
                else:
                    results_df[f'{col}_savings_pct'] = float('nan')

        return results_df

    def print_results(self, results_df: pd.DataFrame):
        """Print formatted benchmark results."""
        print("\n" + "=" * 120)
        print("FILE FORMAT BENCHMARK RESULTS")
        print("=" * 120)

        # Filter out formats with errors
        valid_results = results_df.dropna(subset=['file_size_mb'])

        if valid_results.empty:
            print("No valid benchmark results to display.")
            return

        # Main metrics table
        print(f"Dataset: {self.num_rows:,} rows × 6 columns")
        print(f"DataFrame memory usage: {psutil.virtual_memory().used / (1024**3):.1f} GB system memory")
        print()

        # Format and display results
        display_cols = [
            'format', 'file_size_mb', 'write_time_sec', 'read_time_sec', 'total_time_sec',
            'write_peak_memory_mb', 'read_peak_memory_mb', 'energy_consumption_wh'
        ]

        formatted_results = valid_results[display_cols].copy()

        # Format numeric columns
        formatted_results['file_size_mb'] = formatted_results['file_size_mb'].round(2)
        formatted_results['write_time_sec'] = formatted_results['write_time_sec'].round(3)
        formatted_results['read_time_sec'] = formatted_results['read_time_sec'].round(3)
        formatted_results['total_time_sec'] = formatted_results['total_time_sec'].round(3)
        formatted_results['write_peak_memory_mb'] = formatted_results['write_peak_memory_mb'].round(1)
        formatted_results['read_peak_memory_mb'] = formatted_results['read_peak_memory_mb'].round(1)
        formatted_results['energy_consumption_wh'] = formatted_results['energy_consumption_wh'].round(4)

        print("PERFORMANCE METRICS:")
        print("-" * 120)
        print(formatted_results.to_string(index=False))
        print()

        # Savings table
        if 'file_size_mb_savings_pct' in results_df.columns:
            print("PERCENTAGE SAVINGS VS CSV BASELINE:")
            print("-" * 120)

            savings_cols = [col for col in results_df.columns if col.endswith('_savings_pct')]
            savings_display = ['format'] + savings_cols

            savings_df = results_df[savings_display].copy()
            savings_df = savings_df.dropna(subset=['file_size_mb_savings_pct'])

            # Rename columns for display
            rename_dict = {
                'file_size_mb_savings_pct': 'Size Savings %',
                'write_time_sec_savings_pct': 'Write Time Savings %',
                'read_time_sec_savings_pct': 'Read Time Savings %',
                'total_time_sec_savings_pct': 'Total Time Savings %',
                'write_peak_memory_mb_savings_pct': 'Write Memory Savings %',
                'read_peak_memory_mb_savings_pct': 'Read Memory Savings %',
                'energy_consumption_wh_savings_pct': 'Energy Savings %'
            }

            savings_df = savings_df.rename(columns=rename_dict)
            print(savings_df.to_string(index=False, float_format='%.1f'))

        # Show errors for failed formats
        failed_formats = results_df[results_df['file_size_mb'].isna()]
        if not failed_formats.empty:
            print("\nFAILED FORMATS:")
            print("-" * 120)
            for _, row in failed_formats.iterrows():
                print(f"{row['format']}: {row.get('error', 'Unknown error')}")

        print("\n" + "=" * 120)

    def cleanup(self):
        """Clean up temporary files."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

def main():
    """Main function to run the benchmark."""
    try:
        benchmark = FileFormatBenchmark()  # Use default num_rows (50,000)
        results = benchmark.run_benchmarks()
        benchmark.print_results(results)

    except Exception as e:
        print(f"Benchmark failed: {e}")
        raise
    finally:
        benchmark.cleanup()

if __name__ == "__main__":
    main()