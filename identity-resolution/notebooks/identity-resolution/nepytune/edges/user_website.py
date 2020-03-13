import csv
import json
import logging

from datetime import datetime

from nepytune.write_utils import gremlin_writer, json_lines_file, GremlinEdgeCSV
from nepytune.utils import get_id


logger = logging.getLogger("user_edges")
logger.setLevel(logging.INFO)


def _parse_ts(timestamp):
    """Parse timestamp."""
    for div in (1_000, 1_000_000):
        try:
            return datetime.fromtimestamp(timestamp / div).strftime("%Y-%m-%dT%H:%M:%S")
        except:
            logger.info("Could not parse timestamp: %d with %d", timestamp, div)
    return ""


def generate_user_website_edges(src_map, dst):
    """Generate edges between user nodes and website nodes."""
    with open(src_map["urls"]) as url_file:
        fact_to_website = {}
        for row in csv.reader(url_file, delimiter=","):
            fact_to_website[int(row[0])] = row[1]

    with open(src_map["facts"]) as facts_file:
        attrs = [
            "ts:Date",
            "visited_url:String",
            "uid:String",
            "state:String",
            "city:String",
            "ip_address:String",
        ]
        with gremlin_writer(GremlinEdgeCSV, dst, attributes=attrs) as writer:
            for data in json_lines_file(facts_file):
                for fact in data["facts"]:
                    timestamp = _parse_ts(fact["ts"])
                    website_id = fact_to_website[fact["fid"]]
                    loc_attrs = {
                        "state": fact["state"],
                        "city": fact["city"],
                        "ip_address": fact["ip_address"],
                    }
                    attr_map = {
                        "ts": timestamp,
                        "visited_url": website_id,
                        "uid": data["uid"],
                        **loc_attrs,
                    }
                    user_to_website = {
                        "_id": get_id(data["uid"], website_id, attr_map),
                        "_from": data["uid"],
                        "to": website_id,
                        "label": "visited",
                        "attribute_map": attr_map,
                    }
                    try:
                        writer.add(**user_to_website)
                    except Exception:
                        logger.exception("Something went wrong while creating an edge")
                        logger.info(json.dumps({"uid": data["uid"], **fact}))

    return dst
