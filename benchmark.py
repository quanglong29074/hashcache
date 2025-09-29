#!/usr/bin/env python3
"""
Ultra-high performance HashCache benchmark using asyncio
Target: 18,000-20,000+ ops/sec
"""

import asyncio
import aiohttp
import time
import statistics
import random
import string
import argparse
from typing import List, Dict, Tuple
from dataclasses import dataclass, field

@dataclass
class BenchmarkResults:
    """Thread-safe results storage"""
    put_times: List[float] = field(default_factory=list)
    get_times: List[float] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

class AsyncHashCacheBenchmark:
    def __init__(self, base_url: str = "http://localhost:3000", 
                 concurrency: int = 100,
                 tcp_connector_limit: int = 0):
        self.base_url = base_url
        self.concurrency = concurrency
        self.tcp_connector_limit = tcp_connector_limit if tcp_connector_limit > 0 else None
        
        # Pre-generate data pools
        self.key_pool = self._generate_pool(20000, 10)
        self.value_pool = self._generate_pool(5000, 100)
        
        # Results
        self.results = BenchmarkResults()
        
    def _generate_pool(self, size: int, length: int) -> List[str]:
        """Pre-generate random strings"""
        chars = string.ascii_letters + string.digits
        return [''.join(random.choices(chars, k=length)) for _ in range(size)]
    
    def get_random_data(self) -> Tuple[str, str]:
        """Get random key-value from pools"""
        return (
            self.key_pool[random.randint(0, len(self.key_pool) - 1)],
            self.value_pool[random.randint(0, len(self.value_pool) - 1)]
        )
    
    async def put_operation(self, session: aiohttp.ClientSession, key: str, value: str) -> float:
        """Async PUT operation"""
        start = time.perf_counter()
        try:
            async with session.put(f"{self.base_url}/{key}", data=value, timeout=aiohttp.ClientTimeout(total=10)) as response:
                await response.read()  # Drain response
                if response.status == 200:
                    return time.perf_counter() - start
                else:
                    self.results.errors.append(f"PUT failed with status {response.status}")
                    return -1
        except Exception as e:
            self.results.errors.append(f"PUT error: {str(e)[:50]}")
            return -1
    
    async def get_operation(self, session: aiohttp.ClientSession, key: str) -> float:
        """Async GET operation"""
        start = time.perf_counter()
        try:
            async with session.get(f"{self.base_url}/{key}", timeout=aiohttp.ClientTimeout(total=10)) as response:
                await response.read()  # Drain response
                if response.status == 200:
                    return time.perf_counter() - start
                else:
                    self.results.errors.append(f"GET failed with status {response.status}")
                    return -1
        except Exception as e:
            self.results.errors.append(f"GET error: {str(e)[:50]}")
            return -1
    
    async def worker_mixed_operations(self, session: aiohttp.ClientSession, 
                                     num_operations: int, read_ratio: float = 0.7):
        """Worker coroutine for mixed operations"""
        stored_keys = []
        
        for _ in range(num_operations):
            if stored_keys and random.random() < read_ratio:
                # GET operation
                key = stored_keys[random.randint(0, len(stored_keys) - 1)]
                duration = await self.get_operation(session, key)
                if duration > 0:
                    self.results.get_times.append(duration)
            else:
                # PUT operation
                key, value = self.get_random_data()
                duration = await self.put_operation(session, key, value)
                if duration > 0:
                    self.results.put_times.append(duration)
                    stored_keys.append(key)
                    if len(stored_keys) > 100:
                        stored_keys = stored_keys[-50:]
    
    async def worker_put_only(self, session: aiohttp.ClientSession, num_operations: int):
        """Worker coroutine for PUT only"""
        for _ in range(num_operations):
            key, value = self.get_random_data()
            duration = await self.put_operation(session, key, value)
            if duration > 0:
                self.results.put_times.append(duration)
    
    async def worker_get_only(self, session: aiohttp.ClientSession, 
                             num_operations: int, keys: List[str]):
        """Worker coroutine for GET only"""
        keys_len = len(keys)
        for _ in range(num_operations):
            if keys:
                key = keys[random.randint(0, keys_len - 1)]
                duration = await self.get_operation(session, key)
                if duration > 0:
                    self.results.get_times.append(duration)
    
    async def setup_test_data(self, session: aiohttp.ClientSession, num_keys: int = 1000) -> List[str]:
        """Setup test data asynchronously"""
        print(f"Setting up {num_keys} keys for testing...")
        
        keys = []
        tasks = []
        
        # Create all PUT tasks
        for i in range(num_keys):
            key, value = self.get_random_data()
            tasks.append((key, self.put_operation(session, key, value)))
        
        # Execute in batches to avoid overwhelming the server
        batch_size = self.concurrency * 2
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            results = await asyncio.gather(*[task for _, task in batch])
            
            for j, duration in enumerate(results):
                if duration > 0:
                    keys.append(batch[j][0])
        
        print(f"Successfully set up {len(keys)} keys")
        return keys
    
    async def run_benchmark_mixed(self, total_operations: int, read_ratio: float = 0.7):
        """Run mixed operations benchmark"""
        print(f"Running mixed benchmark ({total_operations} ops, {read_ratio:.1%} reads, {self.concurrency} concurrent)")
        
        connector = aiohttp.TCPConnector(
            limit=self.tcp_connector_limit,
            limit_per_host=self.tcp_connector_limit,
            ttl_dns_cache=300,
            force_close=False,
            enable_cleanup_closed=True
        )
        
        timeout = aiohttp.ClientTimeout(total=None, connect=5, sock_read=10)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            operations_per_worker = total_operations // self.concurrency
            remainder = total_operations % self.concurrency
            
            start_time = time.perf_counter()
            
            tasks = []
            for i in range(self.concurrency):
                ops = operations_per_worker + (1 if i < remainder else 0)
                tasks.append(self.worker_mixed_operations(session, ops, read_ratio))
            
            await asyncio.gather(*tasks)
            
            return time.perf_counter() - start_time
    
    async def run_benchmark_put_only(self, total_operations: int):
        """Run PUT-only benchmark"""
        print(f"Running PUT-only benchmark ({total_operations} ops, {self.concurrency} concurrent)")
        
        connector = aiohttp.TCPConnector(
            limit=self.tcp_connector_limit,
            limit_per_host=self.tcp_connector_limit,
            ttl_dns_cache=300,
            force_close=False,
            enable_cleanup_closed=True
        )
        
        timeout = aiohttp.ClientTimeout(total=None, connect=5, sock_read=10)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            operations_per_worker = total_operations // self.concurrency
            remainder = total_operations % self.concurrency
            
            start_time = time.perf_counter()
            
            tasks = []
            for i in range(self.concurrency):
                ops = operations_per_worker + (1 if i < remainder else 0)
                tasks.append(self.worker_put_only(session, ops))
            
            await asyncio.gather(*tasks)
            
            return time.perf_counter() - start_time
    
    async def run_benchmark_get_only(self, total_operations: int, setup_keys: int = 1000):
        """Run GET-only benchmark"""
        print(f"Running GET-only benchmark ({total_operations} ops, {self.concurrency} concurrent)")
        
        connector = aiohttp.TCPConnector(
            limit=self.tcp_connector_limit,
            limit_per_host=self.tcp_connector_limit,
            ttl_dns_cache=300,
            force_close=False,
            enable_cleanup_closed=True
        )
        
        timeout = aiohttp.ClientTimeout(total=None, connect=5, sock_read=10)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Setup test data
            keys = await self.setup_test_data(session, setup_keys)
            if not keys:
                print("ERROR: No keys were set up!")
                return 0
            
            # Clear setup times
            self.results.put_times.clear()
            self.results.get_times.clear()
            
            operations_per_worker = total_operations // self.concurrency
            remainder = total_operations % self.concurrency
            
            start_time = time.perf_counter()
            
            tasks = []
            for i in range(self.concurrency):
                ops = operations_per_worker + (1 if i < remainder else 0)
                tasks.append(self.worker_get_only(session, ops, keys))
            
            await asyncio.gather(*tasks)
            
            return time.perf_counter() - start_time
    
    def print_results(self, total_time: float, test_name: str):
        """Print formatted results"""
        stats = self.get_stats()
        
        print(f"\n{'='*70}")
        print(f"BENCHMARK RESULTS - {test_name}")
        print(f"{'='*70}")
        print(f"Total execution time: {total_time:.2f} seconds")
        print(f"Concurrency level: {self.concurrency}")
        
        if 'put' in stats:
            put = stats['put']
            print(f"\n📤 PUT Operations ({put['count']:,} total):")
            print(f"  ⚡ Throughput: {put['count']/total_time:,.1f} ops/sec")
            print(f"  ⏱️  Mean latency: {put['mean']:.2f}ms")
            print(f"  📊 Median latency: {put['median']:.2f}ms")
            print(f"  📏 Min/Max: {put['min']:.2f}ms / {put['max']:.2f}ms")
            print(f"  📈 95th percentile: {put['p95']:.2f}ms")
            print(f"  📈 99th percentile: {put['p99']:.2f}ms")
        
        if 'get' in stats:
            get = stats['get']
            print(f"\n📥 GET Operations ({get['count']:,} total):")
            print(f"  ⚡ Throughput: {get['count']/total_time:,.1f} ops/sec")
            print(f"  ⏱️  Mean latency: {get['mean']:.2f}ms")
            print(f"  📊 Median latency: {get['median']:.2f}ms")
            print(f"  📏 Min/Max: {get['min']:.2f}ms / {get['max']:.2f}ms")
            print(f"  📈 95th percentile: {get['p95']:.2f}ms")
            print(f"  📈 99th percentile: {get['p99']:.2f}ms")
        
        total_ops = len(self.results.put_times) + len(self.results.get_times)
        if total_ops > 0:
            print(f"\n🎯 Overall:")
            print(f"  Total operations: {total_ops:,}")
            print(f"  🚀 Overall throughput: {total_ops/total_time:,.1f} ops/sec")
        
        if self.results.errors:
            print(f"\n⚠️  Errors: {len(self.results.errors)}")
            for error in self.results.errors[:5]:
                print(f"  - {error}")
        
        print(f"{'='*70}\n")
    
    def get_stats(self) -> Dict:
        """Calculate statistics"""
        stats = {}
        
        if self.results.put_times:
            put_ms = [t * 1000 for t in self.results.put_times]
            stats['put'] = {
                'count': len(put_ms),
                'mean': statistics.mean(put_ms),
                'median': statistics.median(put_ms),
                'min': min(put_ms),
                'max': max(put_ms),
                'p95': statistics.quantiles(put_ms, n=20)[18] if len(put_ms) >= 20 else max(put_ms),
                'p99': statistics.quantiles(put_ms, n=100)[98] if len(put_ms) >= 100 else max(put_ms),
            }
        
        if self.results.get_times:
            get_ms = [t * 1000 for t in self.results.get_times]
            stats['get'] = {
                'count': len(get_ms),
                'mean': statistics.mean(get_ms),
                'median': statistics.median(get_ms),
                'min': min(get_ms),
                'max': max(get_ms),
                'p95': statistics.quantiles(get_ms, n=20)[18] if len(get_ms) >= 20 else max(get_ms),
                'p99': statistics.quantiles(get_ms, n=100)[98] if len(get_ms) >= 100 else max(get_ms),
            }
        
        return stats
    
    def clear_results(self):
        """Clear all results"""
        self.results.put_times.clear()
        self.results.get_times.clear()
        self.results.errors.clear()

async def test_connection(url: str) -> bool:
    """Test server connectivity"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.put(f"{url}/test_connection", data="test", timeout=aiohttp.ClientTimeout(total=5)) as response:
                return response.status == 200
    except Exception as e:
        print(f"✗ Cannot connect to server at {url}: {e}")
        return False

async def main_async(args):
    """Main async function"""
    if not await test_connection(args.url):
        print("Make sure the HashCache server is running!")
        return 1
    
    print(f"✓ Server is running at {args.url}")
    
    benchmark = AsyncHashCacheBenchmark(
        args.url, 
        args.concurrency,
        args.tcp_limit
    )
    
    if args.test == 'all':
        tests = [
            ('PUT Only', lambda: benchmark.run_benchmark_put_only(args.operations // 2)),
            ('GET Only', lambda: benchmark.run_benchmark_get_only(args.operations // 2, 1000)),
            ('Mixed Operations', lambda: benchmark.run_benchmark_mixed(args.operations, args.read_ratio)),
        ]
        
        for test_name, test_func in tests:
            benchmark.clear_results()
            total_time = await test_func()
            benchmark.print_results(total_time, test_name)
            await asyncio.sleep(1)
    
    elif args.test == 'put':
        total_time = await benchmark.run_benchmark_put_only(args.operations)
        benchmark.print_results(total_time, 'PUT Only')
    
    elif args.test == 'get':
        total_time = await benchmark.run_benchmark_get_only(args.operations, 1000)
        benchmark.print_results(total_time, 'GET Only')
    
    elif args.test == 'mixed':
        total_time = await benchmark.run_benchmark_mixed(args.operations, args.read_ratio)
        benchmark.print_results(total_time, 'Mixed Operations')
    
    return 0

def main():
    parser = argparse.ArgumentParser(description='AsyncIO HashCache Benchmark - Ultra High Performance')
    parser.add_argument('--url', default='http://localhost:3000', help='Server URL')
    parser.add_argument('--concurrency', type=int, default=200, 
                       help='Concurrent workers (default: 200, try 500-1000 for max throughput)')
    parser.add_argument('--tcp-limit', type=int, default=0,
                       help='TCP connection limit (0=unlimited, recommended: 0 or 1000+)')
    parser.add_argument('--operations', type=int, default=50000, 
                       help='Total operations (default: 50000)')
    parser.add_argument('--test', choices=['mixed', 'put', 'get', 'all'], default='all',
                       help='Test type to run')
    parser.add_argument('--read-ratio', type=float, default=0.7,
                       help='Read ratio for mixed test (0.0-1.0)')
    
    args = parser.parse_args()
    
    # Run async main
    return asyncio.run(main_async(args))

if __name__ == '__main__':
    exit(main())