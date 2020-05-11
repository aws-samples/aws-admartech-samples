import hashlib
import inspect
import os

import nepytune.benchmarks.benchmarks_visualization as bench_viz


def hash_(list_):
    """Generate sha1 hash from the given list."""
    return hashlib.sha1(str(tuple(sorted(list_))).encode("utf-8")).hexdigest()


def get_id(_from, to, attributes):
    """Get id of a given entity."""
    return hash_([_from, to, str(tuple(attributes.items()))])


def show_query_benchmarks(benchmark_results_path, cache_path, query,
                          samples_by_users):
    instances = os.listdir(benchmark_results_path)
    instances = sorted(instances, key=lambda x: int(x.split('.')[-1].split('xlarge')[0]))

    benchmarks_dfs = bench_viz.get_benchmarks_results_dataframes(
        query=query,
        samples_by_users=samples_by_users,
        instances=instances,
        results_path=benchmark_results_path
    )
    concurrent_queries_dfs = bench_viz.select_concurrent_queries_from_data(
        query,
        benchmarks_dfs,
        cache_path=cache_path
    )
    bench_viz.show_concurrent_queries_charts(
        concurrent_queries_dfs,
        x_title="Time from start of benchmark (Miliseconds)",
        y_title="Number of concurrent running queries"
    )

    bench_viz.show_query_time_graph(
        benchmarks_dfs,
        yfunc=lambda df: df.multiply(1000).tolist(),
        title="Request duration (Miliseconds)",
        x_title="Number of concurrent queries",
    )
    bench_viz.show_query_time_graph(
        benchmarks_dfs,
        yfunc=lambda df: (1 / df).tolist(),
        title="Queries per second",
        x_title="Number of concurrent queries",
    )


def show(func):
    lines = inspect.getsource(func)
    print(lines)
