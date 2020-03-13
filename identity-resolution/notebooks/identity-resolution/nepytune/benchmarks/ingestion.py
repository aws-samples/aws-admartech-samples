"""Common code for running benchmarks."""

import csv
import json
import logging
import time
import os

import boto3
import botocore
import requests

from itertools import islice

from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import *

import pandas as pd

import plotly.graph_objects as go

from nepytune.benchmarks.drop_graph import drop

AWS_REGION = os.getenv("AWS_REGION")
NEPTUNE_ENDPOINT = os.getenv('NEPTUNE_CLUSTER_ENDPOINT')
NEPTUNE_PORT = os.getenv('NEPTUNE_CLUSTER_PORT')
NEPTUNE_LOADER_ENDPOINT = f"https://{NEPTUNE_ENDPOINT}:{NEPTUNE_PORT}/loader"
NEPTUNE_GREMLIN_ENDPOINT = f"ws://{NEPTUNE_ENDPOINT}:{NEPTUNE_PORT}/gremlin"
NEPTUNE_LOAD_ROLE_ARN = os.getenv("NEPTUNE_LOAD_ROLE_ARN")
BUCKET = os.getenv("S3_PROCESSED_DATASET_BUCKET")
DATASET_DIR = "../../dataset"

GREMLIN_POOL_SIZE       =      8  # Python driver default is 4. Change to create a bigger pool.
GREMLIN_MAX_WORKERS     =      8  # Python driver default is 5 * number of CPU on client machine.

# Initialize Neptune connection
graph=Graph()
connection = DriverRemoteConnection(NEPTUNE_GREMLIN_ENDPOINT,'g',
                                    pool_size=GREMLIN_POOL_SIZE,
                                    max_workers=GREMLIN_MAX_WORKERS)
g = graph.traversal().withRemote(connection)


# Initialize logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


# Make dataset directory
if not os.path.isdir(DATASET_DIR):
    os.mkdir(DATASET_DIR)


def download_file(bucket, file):
    """Download file from S3."""
    try:
        logger.info("Start downloading %s.", file)
        dst = f"./{DATASET_DIR}/{file}"
        if os.path.isfile(dst):
            logger.info("File exists, skipping.")
            return

        s3 = boto3.resource('s3')
        s3.Bucket(bucket).download_file(file, f"./{DATASET_DIR}/{file}")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise


def upload_file(file_name, bucket, prefix, key=None):
    """Upload file to S3 bucket."""
    if key is None:
        key = file_name
    object_name = f"{prefix}/{key}"
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except botocore.exceptions.ClientError as e:
        raise e
    return object_name


def wait_for_load_complete(load_id):
    """Wait for Neptune load to complete."""
    while not is_load_completed(load_id):
        time.sleep(10)


def is_load_completed(load_id):
    """Check if Neptune load is completed"""
    response = requests.get(f"{NEPTUNE_LOADER_ENDPOINT}/{load_id}").json()
    status = response["payload"]["overallStatus"]["status"]
    if status == "LOAD_IN_PROGRESS":
        return False
    return True


def copy_n_lines(src, dst, n):
    """Copy N lines from src to dst file."""
    if os.path.isfile(dst):
        logger.info("File: %s exists, skipping.", dst)
        return

    with open(src) as src_file:
        lines = islice(src_file, n)
        with open(dst, 'w') as dst_file:
            dst_file.writelines(lines)


def populate_graph(vertices_n):
    import tempfile
    import uuid

    logger.info("Populating graph with %s vertices.", vertices_n)

    if vertices_n == 0:
        return

    labels = '"~id","attr1:String","attr2:String","~label"'

    fd, path = tempfile.mkstemp()
    try:
        with os.fdopen(fd, 'w') as tmp:
            tmp.write(labels + '\n')
            for _ in range(vertices_n):
                node_id = str(uuid.uuid4())
                attr1 = node_id
                attr2 = node_id
                label = "generatedVertice"
                tmp.write(f"{node_id},{attr1},{attr2},{label}\n")
            key = upload_file(path, BUCKET, "generated")
            load_into_neptune(BUCKET, key)
            s3 = boto3.resource("s3")
            s3.Object(BUCKET, key).delete()

    finally:
        os.remove(path)



def load_into_neptune(bucket, key):
    """Load CSV file into neptune."""
    data = {
      "source" : f"s3://{bucket}/{key}",
      "format" : "csv",
      "iamRoleArn" : NEPTUNE_LOAD_ROLE_ARN,
      "region" : AWS_REGION,
      "failOnError" : "FALSE",
      "parallelism" : "MEDIUM",
      "updateSingleCardinalityProperties" : "FALSE"
    }
    response = requests.post(NEPTUNE_LOADER_ENDPOINT, json=data)
    json_response = response.json()
    load_id = json_response["payload"]["loadId"]
    logger.info("Waiting for load %s to complete.", load_id)
    wait_for_load_complete(load_id)
    logger.info("Load %s completed", load_id)

    return load_id


def get_loading_time(load_id):
    response = requests.get(f"{NEPTUNE_LOADER_ENDPOINT}/{load_id}").json()
    time_spent = response["payload"]["overallStatus"]["totalTimeSpent"]
    return time_spent


def benchmark_loading_data(source, entities_to_add,
                           initial_sizes=[0], dependencies=[], drop=True):
    """
    Benchmark loading data into AWS Neptune.

    Graph is dropped before every benchmark run.
    Benchmark measures loading time for vertices and edges.
    Graph can be populated with initial random data.
    """

    filename = f"{source}.csv"
    download_file(BUCKET, filename)
    prefix = "splitted"

    results = {}

    logger.info("Loading dependencies.")
    for dependency in dependencies:
        filename = f"{DATASET_DIR}/{dependency}"
        logger.info("Uploading %s to S3 bucket.", dependency)
        key = upload_file(filename, BUCKET, "dependencies", key=dependency)
        load_id = load_into_neptune(BUCKET, key)

    for initial_graph_size in initial_sizes:
        results[initial_graph_size] = {}

        for entities_n in entities_to_add:
            if drop:
                drop(g)
            populate_graph(initial_graph_size)

            logger.info("Generating file with %s entities.", entities_n)
            dst = f"{DATASET_DIR}/{source}_{entities_n}.csv"
            copy_n_lines(f"{DATASET_DIR}/{source}.csv", dst, entities_n)

            logger.info("Uploading %s to S3 bucket.", dst)
            csv_file = upload_file(dst, BUCKET, prefix, f"{source}_{entities_n}.csv")
            load_id = load_into_neptune(BUCKET, csv_file)

            loading_time = get_loading_time(load_id)
            logger.info("Loading %d nodes lasts for %d seconds.", entities_n, loading_time)

            results[initial_graph_size][entities_n] = loading_time

    return results


def save_result_to_csv(source, results, dst="."):
    """Save ingestion results to CSV file."""
    with open(f"{dst}/ingestion-{source}.csv", "w") as f:
        writer = csv.writer(f)
        for initial_size, result in results.items():
            for entites, time in result.items():
                writer.writerow(initial_size, entites, time)


def draw_loading_benchmark_results(results, title, x_title, y_title):
    """Draw loading benchmark results."""
    fig_data = [
        {
            "type": "bar",
            "name": f"Initial graph size: {k}",
            "x": list(v.keys()),
            "y": list(v.values())
        } for k,v in results.items()
    ]

    _draw_group_bar(fig_data, title, x_title, y_title)


def draw_from_csv(csv, title, x_title, y_title):
    """Draw loading benchmark from csv."""
    df = pd.read_csv(csv, names=['initial', 'entities', 'duration'])

    fig_data = [
        {
            "type": "bar",
            "name": f"Initial graph size: {initial_graph_size}",
            "x": group["entities"],
            "y": group["duration"]
        } for initial_graph_size, group in df.groupby('initial')
    ]

    _draw_group_bar(fig_data, title, x_title, y_title)


def _draw_group_bar(fig_data, title, x_title, y_title):
    fig = go.Figure({
        "data": fig_data,
        "layout": {
            "title": {"text": title},
            "xaxis.type": "category",
            "barmode": "group",
            "xaxis_title": x_title,
            "yaxis_title": y_title,
        }
    })

    fig.show()
