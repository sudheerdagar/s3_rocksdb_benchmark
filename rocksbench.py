import os
import time
import uuid
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor
from rocksdict import Rdict

# Constants for benchmarking
NUM_THREADS = 5
FILE_SIZES_MB = [60, 100, 140, 180, 220]  # File sizes for benchmarking
FILE_COUNT = 5

# Generate test data
def generate_test_data(size):
    return os.urandom(size)

# Benchmark RocksDB for different file sizes
def benchmark_rocksdb(file_size_mb):
    db_path = f"./rocksdb_benchmark_{file_size_mb}MB"
    os.makedirs(db_path, exist_ok=True)
    db = Rdict(db_path)

    DATA_SIZE = file_size_mb * 1024 * 1024  # Convert MB to bytes
    test_files = [str(uuid.uuid4()) for _ in range(FILE_COUNT)]

    # Upload files to RocksDB
    start_time = time.time()
    for file in test_files:
        db[file] = generate_test_data(DATA_SIZE)
    upload_time = time.time() - start_time

    # Format upload time to 5 decimals
    upload_time = round(upload_time, 5)

    write_throughput = (FILE_COUNT * DATA_SIZE) / upload_time

    # Multithreaded read operation
    def read_file(file):
        start = time.time()
        data = db[file]
        latency = time.time() - start
        return data, latency

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        results = list(executor.map(read_file, test_files))

    db.close()

    # Calculate metrics
    read_throughput = sum(len(data) for data, _ in results) / upload_time
    latencies = [latency for _, latency in results]
    avg_latency = sum(latencies) / len(latencies)

    # Format metrics to 5 decimals
    read_throughput = round(read_throughput / 1024 / 1024, 5)  # Convert to MB/s and round
    write_throughput = round(write_throughput / 1024 / 1024, 5)  # Convert to MB/s and round
    avg_latency = round(avg_latency, 5)  # Round latency to 5 decimals

    return upload_time, write_throughput, read_throughput, avg_latency

def run_benchmarks():
    upload_times = []
    write_throughputs = []
    read_throughputs = []
    avg_latencies = []

    for file_size in FILE_SIZES_MB:
        print(f"Running benchmark for file size: {file_size} MB...")
        upload_time, write_throughput, read_throughput, avg_latency = benchmark_rocksdb(file_size)
        upload_times.append(upload_time)
        write_throughputs.append(write_throughput)  # Already in MB/s
        read_throughputs.append(read_throughput)  # Already in MB/s
        avg_latencies.append(avg_latency)

    # Plotting the graphs
    plot_graphs(upload_times, write_throughputs, read_throughputs, avg_latencies)

def plot_graphs(upload_times, write_throughputs, read_throughputs, avg_latencies):
    # Plot Upload Time vs File Size
    plt.figure(figsize=(10, 6))
    plt.plot(FILE_SIZES_MB, upload_times, marker='o', color='b', label='Upload Time (s)')
    plt.xlabel('File Size (MB)')
    plt.ylabel('Upload Time (s)')
    plt.title('Upload Time vs File Size')
    plt.grid(True)
    plt.legend()

    # Add annotations for Upload Time
    for i, txt in enumerate(upload_times):
        plt.annotate(f'{txt:.5f}', (FILE_SIZES_MB[i], upload_times[i]), textcoords="offset points", xytext=(0, 5), ha='center', fontsize=10)

    plt.show()

    # Plot Write Throughput vs File Size
    plt.figure(figsize=(10, 6))
    plt.plot(FILE_SIZES_MB, write_throughputs, marker='o', color='g', label='Write Throughput (MB/s)')
    plt.xlabel('File Size (MB)')
    plt.ylabel('Write Throughput (MB/s)')
    plt.title('Write Throughput vs File Size')
    plt.grid(True)
    plt.legend()

    # Add annotations for Write Throughput
    for i, txt in enumerate(write_throughputs):
        plt.annotate(f'{txt:.5f}', (FILE_SIZES_MB[i], write_throughputs[i]), textcoords="offset points", xytext=(0, 5), ha='center', fontsize=10)

    plt.show()

    # Plot Read Throughput vs File Size
    plt.figure(figsize=(10, 6))
    plt.plot(FILE_SIZES_MB, read_throughputs, marker='o', color='r', label='Read Throughput (MB/s)')
    plt.xlabel('File Size (MB)')
    plt.ylabel('Read Throughput (MB/s)')
    plt.title('Read Throughput vs File Size')
    plt.grid(True)
    plt.legend()

    # Add annotations for Read Throughput
    for i, txt in enumerate(read_throughputs):
        plt.annotate(f'{txt:.5f}', (FILE_SIZES_MB[i], read_throughputs[i]), textcoords="offset points", xytext=(0, 5), ha='center', fontsize=10)

    plt.show()

    # Plot Average Read Latency vs File Size
    plt.figure(figsize=(10, 6))
    plt.plot(FILE_SIZES_MB, avg_latencies, marker='o', color='m', label='Average Read Latency (s)')
    plt.xlabel('File Size (MB)')
    plt.ylabel('Average Read Latency (s)')
    plt.title('Average Read Latency vs File Size')
    plt.grid(True)
    plt.legend()

    # Add annotations for Average Read Latency
    for i, txt in enumerate(avg_latencies):
        plt.annotate(f'{txt:.5f}', (FILE_SIZES_MB[i], avg_latencies[i]), textcoords="offset points", xytext=(0, 5), ha='center', fontsize=10)

    plt.show()

if __name__ == "__main__":
    print("Starting RocksDB Benchmark with different file sizes...")
    run_benchmarks()
