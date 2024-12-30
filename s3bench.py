import os
import time
import uuid
import boto3
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

load_dotenv()

# Constants for benchmarking
NUM_THREADS = 5
FILE_SIZES_MB = [60, 100, 140, 180, 220]
FILE_COUNT = 5

# Generate test data
def generate_test_data(size):
    return os.urandom(size)

# Benchmark Amazon S3 on LocalStack
def benchmark_s3():
    # Setup AWS client for LocalStack
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566', region_name='us-east-1')
    bucket_name = "s3-benchmark-bucket"

    # Ensure bucket exists
    try:
        print(f"Creating bucket: {bucket_name}")
        s3.create_bucket(Bucket=bucket_name)
    except s3.exceptions.BucketAlreadyExists:
        print(f"Bucket {bucket_name} already exists.")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket {bucket_name} is already owned by you.")

    results = {
        "file_size": [],
        "upload_time": [],
        "write_throughput": [],
        "read_throughput": [],
        "avg_latency": []
    }

    # Benchmark for each file size
    for file_size_mb in FILE_SIZES_MB:
        data_size = file_size_mb * 1024 * 1024
        test_files = [str(uuid.uuid4()) for _ in range(FILE_COUNT)]

        # Multipart upload function
        def upload_part(file_name, part_number, part_data, upload_id):
            part = s3.upload_part(
                Bucket=bucket_name,
                Key=file_name,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=part_data
            )
            return {'PartNumber': part_number, 'ETag': part['ETag']}

        def multipart_upload(file_name):
            upload_id = s3.create_multipart_upload(Bucket=bucket_name, Key=file_name)['UploadId']
            parts = []
            data = generate_test_data(data_size)
            part_size = 5 * 1024 * 1024
            num_parts = len(data) // part_size + (1 if len(data) % part_size != 0 else 0)
            part_data_list = [data[i * part_size:(i + 1) * part_size] for i in range(num_parts)]

            with ThreadPoolExecutor(max_workers=num_parts) as executor:
                futures = [
                    executor.submit(upload_part, file_name, i + 1, part_data_list[i], upload_id)
                    for i in range(num_parts)
                ]
                for future in futures:
                    parts.append(future.result())

            s3.complete_multipart_upload(
                Bucket=bucket_name,
                Key=file_name,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )

        print(f"Uploading files of size {file_size_mb} MB using multipart upload...")
        start_time = time.time()
        for file in test_files:
            multipart_upload(file)
        upload_time = time.time() - start_time

        write_throughput = (FILE_COUNT * data_size) / upload_time

        print("Reading files...")
        def read_file(file):
            start = time.time()
            obj = s3.get_object(Bucket=bucket_name, Key=file)
            data = obj['Body'].read()
            latency = time.time() - start
            return data, latency

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            results_read = list(executor.map(read_file, test_files))

        read_throughput = sum(len(data) for data, _ in results_read) / upload_time
        latencies = [latency for _, latency in results_read]
        avg_latency = sum(latencies) / len(latencies)

        results["file_size"].append(file_size_mb)
        results["upload_time"].append(upload_time)
        results["write_throughput"].append(write_throughput / 1024 / 1024)
        results["read_throughput"].append(read_throughput / 1024 / 1024)
        results["avg_latency"].append(avg_latency)

        print(f"File Size: {file_size_mb} MB")
        print(f"Upload Time: {upload_time:.5f} seconds")
        print(f"Write Throughput: {write_throughput / 1024 / 1024:.5f} MB/s")
        print(f"Read Throughput: {read_throughput / 1024 / 1024:.5f} MB/s")
        print(f"Average Latency: {avg_latency:.5f} seconds")

    # Plot graphs
    plt.figure(figsize=(12, 8))

    # Upload Time vs File Size
    plt.subplot(2, 2, 1)
    plt.plot(results["file_size"], results["upload_time"], marker='o')
    for x, y in zip(results["file_size"], results["upload_time"]):
        plt.text(x, y, f"{y:.5f}", ha='center', va='bottom')
    plt.title("Upload Time vs File Size")
    plt.xlabel("File Size (MB)")
    plt.ylabel("Upload Time (s)")
    plt.grid()

    # Write Throughput vs File Size
    plt.subplot(2, 2, 2)
    plt.plot(results["file_size"], results["write_throughput"], marker='o')
    for x, y in zip(results["file_size"], results["write_throughput"]):
        plt.text(x, y, f"{y:.5f}", ha='center', va='bottom')
    plt.title("Write Throughput vs File Size")
    plt.xlabel("File Size (MB)")
    plt.ylabel("Write Throughput (MB/s)")
    plt.grid()

    # Read Throughput vs File Size
    plt.subplot(2, 2, 3)
    plt.plot(results["file_size"], results["read_throughput"], marker='o')
    for x, y in zip(results["file_size"], results["read_throughput"]):
        plt.text(x, y, f"{y:.5f}", ha='center', va='bottom')
    plt.title("Read Throughput vs File Size")
    plt.xlabel("File Size (MB)")
    plt.ylabel("Read Throughput (MB/s)")
    plt.grid()

    # Average Latency vs File Size
    plt.subplot(2, 2, 4)
    plt.plot(results["file_size"], results["avg_latency"], marker='o')
    for x, y in zip(results["file_size"], results["avg_latency"]):
        plt.text(x, y, f"{y:.5f}", ha='center', va='bottom')
    plt.title("Average Latency vs File Size")
    plt.xlabel("File Size (MB)")
    plt.ylabel("Average Latency (s)")
    plt.grid()

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    print("Starting Amazon S3 Benchmark on LocalStack...")
    benchmark_s3()
