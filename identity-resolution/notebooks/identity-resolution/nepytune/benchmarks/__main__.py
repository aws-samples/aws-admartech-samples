import argparse
import asyncio
import csv
import logging
import os
import random
import time
import statistics

import numpy as np

from nepytune.benchmarks.query_runner import get_query_runner
from nepytune.benchmarks.connection_pool import NeptuneConnectionPool

QUERY_NAMES = [
    'get_sibling_attrs', 'undecided_user_check', 'undecided_user_audience',
    'brand_interaction_audience', 'get_all_transient_ids_in_household',
    'early_website_adopters'
]

parser = argparse.ArgumentParser(description="Run query benchmarks")
parser.add_argument("--users", type=int, default=10)
parser.add_argument("--samples", type=int, default=1000)
parser.add_argument("--queries", default=['all'], type=str,
    nargs='+', choices=QUERY_NAMES + ['all'])
parser.add_argument("--verbose", action='store_true')
parser.add_argument("--csv", action="store_true")
parser.add_argument("--output", type=str, default="results")
args = parser.parse_args()

if args.queries == ['all']:
    args.queries = QUERY_NAMES

if (args.verbose):
    level = logging.DEBUG
else:
    level = logging.INFO

logging.basicConfig(level=level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

sem = asyncio.Semaphore(args.users)


def custom_exception_handler(loop, context):
    """Stop event loop if exception occurs."""
    loop.default_exception_handler(context)

    exception = context.get('exception')
    if isinstance(exception, Exception):
        print(context)
        loop.stop()


async def run_query(query_runner, sample, semaphore, pool):
    """Run query with limit on concurrent connections."""
    async with semaphore:
        return await query_runner.run(sample, pool)


async def run(query, samples, pool):
    """Run query benchmark tasks."""
    query_runner = get_query_runner(query, samples)

    logger.info("Initializing query data.")
    await asyncio.gather(query_runner.initialize())

    queries = []
    logger.info("Running benchmark.")
    for i in range(samples):
        queries.append(asyncio.create_task(run_query(query_runner, i, sem, pool)))
    results = await asyncio.gather(*queries)

    logger.info(f"Successful queries: {query_runner.succeded}")
    logger.info(f"Failed queries: {query_runner.failed}")

    benchmark_results = [result for result in results if result]
    return benchmark_results, query_runner.succeded, query_runner.failed


def stats(results):
    """Print statistics for benchmark results."""
    print(f"Samples: {args.samples}")
    print(f"Mean: {statistics.mean(results)}s")
    print(f"Median: {statistics.median(results)}s")
    a = np.array(results)
    for percentile in [50, 90, 99, 99.9, 99.99]:
        result = np.percentile(a, percentile)
        print(f"{percentile} percentile: {result}s")


if __name__ ==  '__main__':
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(custom_exception_handler)

    pool = NeptuneConnectionPool(args.users)
    try:
        loop.run_until_complete(pool.create())
        for query in args.queries:
            logger.info(f"Benchmarking query: {query}")
            logger.info(f"Concurrent users: {args.users}")
            results, succeded, failed = loop.run_until_complete(run(query, args.samples, pool))
            stats([measure[2] for measure in results])
            if args.csv:
                dst = f"{args.output}/{query}-{args.samples}-{args.users}.csv"
                with open(dst, "w") as f:
                    writer = csv.writer(f)
                    for measure in results:
                        writer.writerow(measure)
                query_stats = f"{args.output}/{query}-{args.samples}-{args.users}-stats.csv"
                with open(query_stats, "w") as f:
                    writer = csv.writer(f)
                    writer.writerow([succeded, failed])
    finally:
        loop.run_until_complete(pool.destroy())
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
