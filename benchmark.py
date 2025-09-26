#!/usr/bin/env python3
"""
Python benchmark for HashCache server using multiple threads
Tests PUT, GET operations and measures performance metrics
"""

import requests
import threading
import time
import statistics
import random
import string
import json
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple

class HashCacheBenchmark:
    def __init__(self, base_url: str = "http://localhost:3000", num_threads: int = 10):
        self.base_url = base_url
        self.num_threads = num_threads
        self.session = requests.Session()
        
        # Results storage
        self.put_times: List[float] = []
        self.get_times: List[float] = []
        self.errors: List[str] = []
        self.lock = threading.Lock()
        
    def generate_random_data(self, key_length: int = 10, value_length: int = 100) -> Tuple[str, str]:
        """Generate random key-value pair"""
        key = ''.join(random.choices(string.ascii_letters + string.digits, k=key_length))
        value = ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + ' ', k=value_length))
        return key, value
    
    def put_operation(self, key: str, value: str) -> float:
        """Perform PUT operation and return response time"""
        start_time = time.time()
        try:
            response = self.session.put(f"{self.base_url}/{key}", data=value)
            response.raise_for_status()
            return time.time() - start_time
        except requests.RequestException as e:
            with self.lock:
                self.errors.append(f"PUT error for key {key}: {str(e)}")
            return -1
    
    def get_operation(self, key: str) -> float:
        """Perform GET operation and return response time"""
        start_time = time.time()
        try:
            response = self.session.get(f"{self.base_url}/{key}")
            response.raise_for_status()
            return time.time() - start_time
        except requests.RequestException as e:
            with self.lock:
                self.errors.append(f"GET error for key {key}: {str(e)}")
            return -1
    
    def worker_thread_mixed_operations(self, thread_id: int, operations_per_thread: int, read_ratio: float = 0.7):
        """Worker thread that performs mixed PUT/GET operations"""
        thread_put_times = []
        thread_get_times = []
        stored_keys = []
        
        for i in range(operations_per_thread):
            # Decide operation type based on read_ratio and available keys
            if stored_keys and random.random() < read_ratio:
                # Perform GET operation
                key = random.choice(stored_keys)
                response_time = self.get_operation(key)
                if response_time > 0:
                    thread_get_times.append(response_time)
            else:
                # Perform PUT operation
                key, value = self.generate_random_data()
                response_time = self.put_operation(key, value)
                if response_time > 0:
                    thread_put_times.append(response_time)
                    stored_keys.append(key)
                    # Limit stored keys to prevent memory issues
                    if len(stored_keys) > 100:
                        stored_keys = stored_keys[-50:]
        
        # Store results thread-safely
        with self.lock:
            self.put_times.extend(thread_put_times)
            self.get_times.extend(thread_get_times)
    
    def worker_thread_put_only(self, thread_id: int, operations_per_thread: int):
        """Worker thread that performs only PUT operations"""
        thread_times = []
        
        for i in range(operations_per_thread):
            key, value = self.generate_random_data()
            response_time = self.put_operation(key, value)
            if response_time > 0:
                thread_times.append(response_time)
        
        with self.lock:
            self.put_times.extend(thread_times)
    
    def worker_thread_get_only(self, thread_id: int, operations_per_thread: int, keys: List[str]):
        """Worker thread that performs only GET operations on existing keys"""
        thread_times = []
        
        for i in range(operations_per_thread):
            if keys:
                key = random.choice(keys)
                response_time = self.get_operation(key)
                if response_time > 0:
                    thread_times.append(response_time)
        
        with self.lock:
            self.get_times.extend(thread_times)
    
    def setup_test_data(self, num_keys: int = 1000) -> List[str]:
        """Setup initial test data for GET benchmarks"""
        print(f"Setting up {num_keys} keys for testing...")
        keys = []
        
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            futures = []
            
            for i in range(num_keys):
                key, value = self.generate_random_data()
                future = executor.submit(self.put_operation, key, value)
                futures.append((future, key))
            
            for future, key in futures:
                try:
                    result = future.result(timeout=30)
                    if result > 0:
                        keys.append(key)
                except Exception as e:
                    self.errors.append(f"Setup error for key {key}: {str(e)}")
        
        print(f"Successfully set up {len(keys)} keys")
        return keys
    
    def run_benchmark_mixed_operations(self, total_operations: int = 10000, read_ratio: float = 0.7):
        """Run benchmark with mixed PUT/GET operations"""
        print(f"Running mixed operations benchmark ({total_operations} operations, {read_ratio:.1%} reads)")
        
        operations_per_thread = total_operations // self.num_threads
        remainder = total_operations % self.num_threads
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            futures = []
            
            for i in range(self.num_threads):
                ops = operations_per_thread + (1 if i < remainder else 0)
                future = executor.submit(self.worker_thread_mixed_operations, i, ops, read_ratio)
                futures.append(future)
            
            # Wait for all threads to complete
            for future in as_completed(futures):
                try:
                    future.result(timeout=300)  # 5 minute timeout per thread
                except Exception as e:
                    self.errors.append(f"Thread execution error: {str(e)}")
        
        total_time = time.time() - start_time
        return total_time
    
    def run_benchmark_put_only(self, total_operations: int = 5000):
        """Run benchmark with only PUT operations"""
        print(f"Running PUT-only benchmark ({total_operations} operations)")
        
        operations_per_thread = total_operations // self.num_threads
        remainder = total_operations % self.num_threads
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            futures = []
            
            for i in range(self.num_threads):
                ops = operations_per_thread + (1 if i < remainder else 0)
                future = executor.submit(self.worker_thread_put_only, i, ops)
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    future.result(timeout=300)
                except Exception as e:
                    self.errors.append(f"Thread execution error: {str(e)}")
        
        total_time = time.time() - start_time
        return total_time
    
    def run_benchmark_get_only(self, total_operations: int = 5000, setup_keys: int = 1000):
        """Run benchmark with only GET operations"""
        print(f"Running GET-only benchmark ({total_operations} operations)")
        
        # Setup test data first
        keys = self.setup_test_data(setup_keys)
        if not keys:
            print("ERROR: No keys were set up successfully!")
            return 0
        
        # Clear previous timing data since setup added PUT times
        with self.lock:
            self.put_times.clear()
            self.get_times.clear()
        
        operations_per_thread = total_operations // self.num_threads
        remainder = total_operations % self.num_threads
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            futures = []
            
            for i in range(self.num_threads):
                ops = operations_per_thread + (1 if i < remainder else 0)
                future = executor.submit(self.worker_thread_get_only, i, ops, keys)
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    future.result(timeout=300)
                except Exception as e:
                    self.errors.append(f"Thread execution error: {str(e)}")
        
        total_time = time.time() - start_time
        return total_time
    
    def get_stats(self) -> Dict:
        """Calculate and return performance statistics"""
        stats = {}
        
        # Server stats
        try:
            response = self.session.get(f"{self.base_url}/stats")
            if response.status_code == 200:
                try:
                    stats['server'] = response.json()
                except:
                    # If JSON parsing fails, create a manual count
                    stats['server'] = {'total_keys': 'unknown (server running)'}
            else:
                stats['server'] = {'total_keys': 'unknown (server running)'}
        except:
            stats['server'] = {'error': 'Could not retrieve server stats'}
        
        # PUT operation stats
        if self.put_times:
            stats['put'] = {
                'count': len(self.put_times),
                'mean': statistics.mean(self.put_times) * 1000,  # Convert to milliseconds
                'median': statistics.median(self.put_times) * 1000,
                'min': min(self.put_times) * 1000,
                'max': max(self.put_times) * 1000,
                'std_dev': statistics.stdev(self.put_times) * 1000 if len(self.put_times) > 1 else 0,
                'p95': statistics.quantiles(self.put_times, n=20)[18] * 1000 if len(self.put_times) >= 20 else max(self.put_times) * 1000,
                'p99': statistics.quantiles(self.put_times, n=100)[98] * 1000 if len(self.put_times) >= 100 else max(self.put_times) * 1000,
            }
        
        # GET operation stats
        if self.get_times:
            stats['get'] = {
                'count': len(self.get_times),
                'mean': statistics.mean(self.get_times) * 1000,
                'median': statistics.median(self.get_times) * 1000,
                'min': min(self.get_times) * 1000,
                'max': max(self.get_times) * 1000,
                'std_dev': statistics.stdev(self.get_times) * 1000 if len(self.get_times) > 1 else 0,
                'p95': statistics.quantiles(self.get_times, n=20)[18] * 1000 if len(self.get_times) >= 20 else max(self.get_times) * 1000,
                'p99': statistics.quantiles(self.get_times, n=100)[98] * 1000 if len(self.get_times) >= 100 else max(self.get_times) * 1000,
            }
        
        stats['errors'] = {
            'count': len(self.errors),
            'details': self.errors[:10]  # Show first 10 errors
        }
        
        return stats
    
    def print_results(self, total_time: float, test_name: str = ""):
        """Print formatted benchmark results"""
        stats = self.get_stats()
        
        print(f"\n{'='*60}")
        print(f"BENCHMARK RESULTS - {test_name}")
        print(f"{'='*60}")
        print(f"Total execution time: {total_time:.2f} seconds")
        print(f"Threads used: {self.num_threads}")
        
        if 'server' in stats and 'total_keys' in stats['server']:
            print(f"Total keys in server: {stats['server']['total_keys']}")
        
        if 'put' in stats:
            put = stats['put']
            print(f"\nPUT Operations ({put['count']} total):")
            print(f"  Throughput: {put['count']/total_time:.1f} ops/sec")
            print(f"  Mean latency: {put['mean']:.2f}ms")
            print(f"  Median latency: {put['median']:.2f}ms")
            print(f"  Min/Max latency: {put['min']:.2f}ms / {put['max']:.2f}ms")
            print(f"  95th percentile: {put['p95']:.2f}ms")
            print(f"  99th percentile: {put['p99']:.2f}ms")
            print(f"  Std deviation: {put['std_dev']:.2f}ms")
        
        if 'get' in stats:
            get = stats['get']
            print(f"\nGET Operations ({get['count']} total):")
            print(f"  Throughput: {get['count']/total_time:.1f} ops/sec")
            print(f"  Mean latency: {get['mean']:.2f}ms")
            print(f"  Median latency: {get['median']:.2f}ms")
            print(f"  Min/Max latency: {get['min']:.2f}ms / {get['max']:.2f}ms")
            print(f"  95th percentile: {get['p95']:.2f}ms")
            print(f"  99th percentile: {get['p99']:.2f}ms")
            print(f"  Std deviation: {get['std_dev']:.2f}ms")
        
        total_ops = len(self.put_times) + len(self.get_times)
        if total_ops > 0:
            print(f"\nOverall:")
            print(f"  Total operations: {total_ops}")
            print(f"  Overall throughput: {total_ops/total_time:.1f} ops/sec")
        
        if stats['errors']['count'] > 0:
            print(f"\nErrors: {stats['errors']['count']}")
            for error in stats['errors']['details']:
                print(f"  - {error}")
        
        print(f"{'='*60}\n")

def main():
    parser = argparse.ArgumentParser(description='HashCache Python Benchmark')
    parser.add_argument('--url', default='http://localhost:3000', help='Server URL')
    parser.add_argument('--threads', type=int, default=10, help='Number of threads')
    parser.add_argument('--operations', type=int, default=10000, help='Total operations')
    parser.add_argument('--test', choices=['mixed', 'put', 'get', 'all'], default='all',
                       help='Test type to run')
    parser.add_argument('--read-ratio', type=float, default=0.7,
                       help='Read ratio for mixed test (0.0-1.0)')
    
    args = parser.parse_args()
    
    # Test server connectivity with a simple PUT/GET test
    try:
        test_response = requests.put(f"{args.url}/test_connection", data="test", timeout=5)
        if test_response.status_code == 200:
            print(f"✓ Server is running at {args.url}")
        else:
            raise requests.RequestException(f"Unexpected response: {test_response.status_code}")
    except requests.RequestException as e:
        print(f"✗ Cannot connect to server at {args.url}: {e}")
        print("Make sure the HashCache server is running!")
        return 1
    
    benchmark = HashCacheBenchmark(args.url, args.threads)
    
    if args.test == 'all':
        # Run all benchmarks
        tests = [
            ('PUT Only', lambda: benchmark.run_benchmark_put_only(args.operations // 2)),
            ('GET Only', lambda: benchmark.run_benchmark_get_only(args.operations // 2, 1000)),
            ('Mixed Operations', lambda: benchmark.run_benchmark_mixed_operations(args.operations, args.read_ratio)),
        ]
        
        for test_name, test_func in tests:
            benchmark.put_times.clear()
            benchmark.get_times.clear()
            benchmark.errors.clear()
            
            total_time = test_func()
            benchmark.print_results(total_time, test_name)
            
            # Brief pause between tests
            time.sleep(2)
    
    elif args.test == 'put':
        total_time = benchmark.run_benchmark_put_only(args.operations)
        benchmark.print_results(total_time, 'PUT Only')
    
    elif args.test == 'get':
        total_time = benchmark.run_benchmark_get_only(args.operations, 1000)
        benchmark.print_results(total_time, 'GET Only')
    
    elif args.test == 'mixed':
        total_time = benchmark.run_benchmark_mixed_operations(args.operations, args.read_ratio)
        benchmark.print_results(total_time, 'Mixed Operations')
    
    return 0

if __name__ == '__main__':
    exit(main())
