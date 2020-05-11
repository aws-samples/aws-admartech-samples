import datetime
import math
import os
import tqdm
import pandas as pd
import plotly.graph_objects as go
from intervaltree import IntervalTree
from plotly.subplots import make_subplots


def get_benchmarks_results_dataframes(results_path, query, instances,
                                      samples_by_users):
    """Convert benchmarks results into data frames."""
    dfs_by_users = {}
    for users, samples in samples_by_users.items():
        dfs = []
        for instance in instances:
            df = pd.read_csv(f"{results_path}/{instance}/{query}-{samples}-{users}.csv",
                             names=['start', 'end', 'duration'])
            df["instance"] = instance
            dfs.append(df)

        dfs_by_users[users] = pd.concat(dfs)
    return dfs_by_users


def show_query_time_graph(benchmarks_dfs, yfunc, title, x_title):
    """Show query duration graph."""
    fig = go.Figure()

    for users, df in benchmarks_dfs.items():
        fig.add_trace(
            go.Box(
                x=df["instance"],
                y=yfunc(df["duration"]),
                boxpoints=False,
                boxmean=True,
                name=f"{users} users",
                hoverinfo="y",
            )
        )

    fig.update_layout(
        yaxis=dict(
            title=title,
            tickangle=-45,
        ),
        xaxis_title=x_title,
        boxmode='group'
    )
    fig.show()


def select_concurrent_queries_from_data(query, benchmarks_dfs, cache_path):
    """Measure concurrent queries from benchmark results."""
    users_chart_data = {}
    cache_suffix = "cache_concurrent"

    if not os.path.isdir(cache_path):
        os.makedirs(cache_path)

    for users in benchmarks_dfs.keys():
        cache_filename = f"{cache_path}/{query}-{users}-{cache_suffix}.csv"
        if os.path.isfile(cache_filename):
            with open(cache_filename) as f:
                print(f"Reading from cached file: {cache_filename}.")
                queries_df = pd.read_csv(f)
                queries_df = queries_df.set_index(
                    pd.to_datetime(queries_df['timestamp']))
                users_chart_data[users] = queries_df
        else:
            df = benchmarks_dfs[users].copy()
            # convert to milliseconds
            df["duration"] = df["duration"].multiply(1000)

            data_frames = []
            for instance in df.instance.unique():
                queries = get_concurrent_queries_by_time(df, users, instance)
                queries_df = pd.DataFrame(
                    queries, columns=['timestamp', 'users', 'instance'])

                resampled = resample_queries_frame(queries_df, '100ms')

                data_frames.append(resampled)

            with open(cache_filename, "w") as f:
                pd.concat(data_frames).to_csv(f)

            users_chart_data[users] = pd.concat(data_frames)

    return users_chart_data


def show_concurrent_queries_charts(concurrent_queries_dfs, x_title, y_title):
    """Show concurrent queries chart."""
    for users, df in concurrent_queries_dfs.items():
        instances = len(df.instance.unique())

        fig = make_subplots(rows=instances, cols=1)

        for row, instance in enumerate(df.instance.unique(), start=1):
            instance_data = df[df.instance == instance]
            fig.add_trace(
                go.Scatter(
                    x=[(idx - instance_data.index[0]).total_seconds()
                       for idx in instance_data.index],
                    y=instance_data["users"],
                    name=instance
                ),
                row=row,
                col=1
            )

        fig.update_yaxes(
            title_text=f"{y_title} for: {users} users", row=2, col=1)
        fig.update_xaxes(title_text=x_title, row=3, col=1)

        fig.show()


def get_concurrent_queries_by_time(df, users, instance):
    """
    Return concurrent running queries by time.

    Build interval tree of running query times.
    Calculate time range duration and check overlaping queries.
    """
    idf = df.loc[df["instance"] == instance].copy()

    idf['start'] = pd.to_datetime(idf['start'], unit='s')
    idf['end'] = pd.to_datetime(idf['end'], unit='s')

    # get nsmallest and nlargest to not leave single running queries
    start = idf.nsmallest(int(users), "start")["start"].max()
    end = idf.nlargest(int(users), "end")["end"].min()

    step = math.ceil(idf['duration'].min()/10)

    t = IntervalTree()
    for index, row in idf.iterrows():
        t[row["start"]:row["end"]] = None

    tr = pd.to_datetime(pd.date_range(
        start=start, end=end, freq=f"{step}ms"))

    rows = []
    for i in tqdm.tqdm(range(len(tr)-1)):
        r1 = tr[i]
        r2 = tr[i+1]
        concurrent_queries = len(t[r1:r2])
        rows.append([r1, concurrent_queries, instance])

    return rows


def resample_queries_frame(df, freq):
    """Resample queries frame with given frequency."""
    df = df.set_index(pd.to_datetime(df['timestamp']))

    resampled = pd.DataFrame()
    resampled["users"] = df.users.resample(freq).mean().bfill()
    resampled["instance"] = df.instance.resample(freq).last().bfill()

    return resampled
