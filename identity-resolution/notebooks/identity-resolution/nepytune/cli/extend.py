import argparse
import logging
import configparser
import os
import json
import itertools
import random

from nepytune.write_utils import json_lines_file


logger = logging.getLogger("extend")
logger.setLevel(logging.INFO)


def extend_facts_file(fact_file_path, ip_loc_file_path, user_identity_file_path):
    """Extend facts file with additional information."""
    ip_loc_cor = extend_with_iploc_information(ip_loc_file_path)
    user_identity_cor = extend_with_user_identity_information(user_identity_file_path)

    next(ip_loc_cor)
    next(user_identity_cor)

    dst = f"{fact_file_path}.tmp"
    with open(fact_file_path) as f_h:
        with open(dst, "w") as f_dst:
            for data in json_lines_file(f_h):
                transformed_row = user_identity_cor.send(ip_loc_cor.send(data))
                f_dst.write(json.dumps(transformed_row) + "\n")

        ip_loc_cor.close()

    os.rename(dst, fact_file_path)


def extend_with_user_identity_information(user_identity_file_path):
    """Coroutine which generates user identity facts based on transient id."""
    with open(user_identity_file_path) as f_h:
        user_id_data = {data["transient_id"]: data for data in json_lines_file(f_h)}

    data = yield

    while data is not None:
        transformed = {**data.copy(), **user_id_data[data["uid"]]}
        del transformed["transient_id"]
        data = yield transformed


def extend_with_iploc_information(ip_loc_file_path):
    """Coroutine which generates ip location facts based on transient id."""
    with open(ip_loc_file_path) as f_h:
        loc_data = {data["transient_id"]: data["loc"] for data in json_lines_file(f_h)}

    data = yield

    def get_sane_ip_locaction(uid, facts, max_ts_difference=3600):
        """
        Given transient id and its facts add information about ip/location.

        Process is semi-deterministic.
            1. Choose the location at random from the given list of locations
            2. Repeat returning this location as long as the timestamp difference
               lies within the `max_ts_difference`
            3. Otherwise, start from 1)
        """
        facts = [None] + sorted(facts, key=lambda x: x["ts"])
        ptr1, ptr2 = itertools.tee(facts, 2)
        next(ptr2, None)

        loc_fact = random.choice(loc_data[uid])

        for previous_item, current in zip(ptr1, ptr2):
            if (
                previous_item is None
                or current["ts"] - previous_item["ts"] > max_ts_difference
            ):
                loc_fact = random.choice(loc_data[uid])
            yield {**current, **loc_fact}

    while data is not None:
        transformed = data.copy()
        transformed["facts"] = list(
            get_sane_ip_locaction(uid=data["uid"], facts=data["facts"])
        )
        data = yield transformed


def register(parser):
    """Register 'extend' parser."""
    extend_parser = parser.add_parser("extend")
    extend_parser.set_defaults(subparser="extend")
    extend_parser.add_argument(
        "--config-file", type=argparse.FileType("r"), required=True
    )

    extend_subparser = extend_parser.add_subparsers()
    _ = extend_subparser.add_parser("facts")
    extend_parser.set_defaults(command="facts")


def main(args):
    """Extend facts with information about the world."""
    config = configparser.ConfigParser()
    config.read(args.config_file.name)

    if args.command == "facts":
        logger.info("Extend facts file to %s", config["src"]["facts"])
        extend_facts_file(
            fact_file_path=config["src"]["facts"],
            ip_loc_file_path=config["dst"]["ip_info"],
            user_identity_file_path=config["dst"]["user_identity_info"],
        )

    logger.info("Done!")
