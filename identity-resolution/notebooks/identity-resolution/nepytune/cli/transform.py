import argparse
import logging
import configparser
import glob
from pathlib import PurePath
import concurrent.futures
from string import Template

from nepytune.nodes import websites, users, identity_groups, ip_loc
from nepytune.edges import (
    user_website,
    website_groups,
    identity_groups as identity_group_edges,
    persistent_ids,
    ip_loc as ip_loc_edges,
)


logger = logging.getLogger("transform")
logger.setLevel(logging.INFO)


def build_destination_path(src, dst):
    """Given src path, extract batch information and build new destination path."""
    stem = PurePath(src).stem
    batch_id = f"{'_'.join(stem.split('_')[:2])}_"
    return Template(dst).substitute(batch_id=batch_id)


def register(parser):
    """Register 'transform' parser."""
    transform_parser = parser.add_parser("transform")
    transform_parser.set_defaults(subparser="transform")

    transform_parser.add_argument(
        "--config-file", type=argparse.FileType("r"), required=True
    )
    transform_parser.add_argument("--websites", action="store_true", default=False)
    transform_parser.add_argument("--website_groups", action="store_true", default=False)
    transform_parser.add_argument("--transientIds", action="store_true", default=False)
    transform_parser.add_argument("--persistentIds", action="store_true", default=False)
    transform_parser.add_argument(
        "--identityGroupIds", action="store_true", default=False
    )
    transform_parser.add_argument("--ips", action="store_true", default=False)
    # workers param affect only processing transient entities;
    # other types of entities are processed fast enough
    transform_parser.add_argument("--workers", type=int, default=1)


def main(args):
    """Transform csv files into ready-to-load neptune format."""
    config = configparser.ConfigParser()
    config.read(args.config_file.name)

    files = {
        "facts": config["src"]["facts"],
        "urls": config["src"]["urls"],
        "titles": config["src"]["titles"],
    }

    if args.websites:
        logger.info("Generating website nodes to %s", config["dst"]["websites"])
        websites.generate_website_nodes(
            files["urls"], files["titles"], config["dst"]["websites"]
        )

    if args.website_groups:
        groups_json = config["src"]["website_groups"]

        nodes_dst = config["dst"]["website_group_nodes"]
        logger.info("Generating website group nodes to %s", nodes_dst)
        websites.generate_website_group_nodes(groups_json, nodes_dst)

        edges_dst = config["dst"]["website_group_edges"]
        logger.info("Generating website group edges to %s", edges_dst)
        website_groups.generate_website_group_edges(groups_json, edges_dst)

    if args.transientIds:
        if args.workers > 1:
            fact_files = sorted(glob.glob(config["src"]["facts_glob"]))
            url_files = sorted(glob.glob(config["src"]["urls_glob"]))

            with concurrent.futures.ProcessPoolExecutor(
                max_workers=args.workers
            ) as executor:
                futures = []
                logger.info("Scheduling...")
                for fact_file, url_file in zip(fact_files, url_files):
                    futures.append(
                        executor.submit(
                            users.generate_user_nodes,
                            fact_file,
                            build_destination_path(
                                fact_file, config["dst"]["transient_nodes"]
                            ),
                        )
                    )
                    futures.append(
                        executor.submit(
                            user_website.generate_user_website_edges,
                            {
                                "titles": files["titles"],
                                "urls": url_file,
                                "facts": fact_file,
                            },
                            build_destination_path(
                                fact_file, config["dst"]["transient_edges"]
                            ),
                        )
                    )
                logger.info("Processing of transient nodes started.")

                for future in concurrent.futures.as_completed(futures):
                    logger.info(
                        "Succesfully written transient entity file into %s",
                        future.result(),
                    )
        else:
            nodes_dst = Template(config["dst"]["transient_nodes"]).substitute(
                batch_id=""
            )
            logger.info("Generating transient id nodes to %s", nodes_dst)
            users.generate_user_nodes(config["src"]["facts"], nodes_dst)

            edges_dst = Template(config["dst"]["transient_edges"]).substitute(
                batch_id=""
            )
            logger.info("Generating transient id edges to %s", edges_dst)
            user_website.generate_user_website_edges(files, edges_dst)

    if args.persistentIds:
        logger.info(
            "Generating persistent id nodes to %s", config["dst"]["persistent_nodes"]
        )
        users.generate_persistent_nodes(
            config["src"]["persistent"], config["dst"]["persistent_nodes"]
        )
        logger.info(
            "Generating persistent id edges to %s", config["dst"]["persistent_edges"]
        )
        persistent_ids.generate_persistent_id_edges(
            config["src"]["persistent"], config["dst"]["persistent_edges"]
        )

    if args.identityGroupIds:
        logger.info(
            "Generating identity group id nodes to %s",
            config["dst"]["identity_group_nodes"],
        )
        identity_groups.generate_identity_group_nodes(
            config["src"]["identity_group"], config["dst"]["identity_group_nodes"]
        )
        logger.info(
            "Generating identity group id edges to %s",
            config["dst"]["identity_group_edges"],
        )
        identity_group_edges.generate_identity_group_edges(
            config["src"]["identity_group"], config["dst"]["identity_group_edges"]
        )

    if args.ips:
        logger.info("Generating IP id nodes to %s", config["dst"]["ip_nodes"])
        ip_loc.generate_ip_loc_nodes_from_facts(
            config["src"]["facts"], config["dst"]["ip_nodes"]
        )
        logger.info("Generating IP edges to %s", config["dst"]["ip_edges"])
        ip_loc_edges.generate_ip_loc_edges_from_facts(
            config["src"]["facts"], config["dst"]["ip_edges"]
        )

    logger.info("Done!")
