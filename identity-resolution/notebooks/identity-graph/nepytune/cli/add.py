import argparse
import configparser
import json
import time
import random
import sys
import csv
import logging
import ipaddress
from collections import namedtuple
from urllib.parse import urlparse

from faker import Faker
from faker.providers.user_agent import Provider as UAProvider
from user_agents import parse


from networkx.utils.union_find import UnionFind

from nepytune.write_utils import json_lines_file
from nepytune.utils import hash_


COMPANY_MIN_SIZE = 6

logger = logging.getLogger("add")
logger.setLevel(logging.INFO)


class UserAgentProvider(UAProvider):
    """Custom faker provider that derives user agent based on type."""

    def user_agent_from_type(self, type_):
        """Given type, generate appropriate user agent."""
        while True:
            user_agent = self.user_agent()
            if type_ == "device":
                if "Mobile" in user_agent:
                    return user_agent
            elif type_ == "cookie":
                if "Mobile" not in user_agent:
                    return user_agent
            else:
                raise ValueError(f"Unsupported {type_}")


class PersistentNodes(UnionFind):
    """networkx.UnionFind datastructure with custom iterable over node sets."""

    def node_groups(self):
        """Iterate over node groups yield parent hash and node members."""
        for node_set in self.to_sets():
            yield hash_(node_set), node_set


def extract_user_groups(user_mapping_path):
    """Generate disjoint user groups based on union find datastructure."""
    with open(user_mapping_path) as f_h:
        pers_reader = csv.reader(f_h, delimiter=",")
        uf_ds = PersistentNodes()
        for row in pers_reader:
            uf_ds.union(row[0], row[1])
        return uf_ds


def generate_persistent_groups(user_groups, dst):
    """Write facts about persistent to transient nodes mapping."""
    with open(dst, "w") as f_h:
        for persistent_id, node_group in user_groups.node_groups():
            f_h.write(
                json.dumps({"pid": persistent_id, "transientIds": list(node_group)})
                + "\n"
            )


def generate_identity_groups(persistent_ids_file, distribution, dst, _seed=None):
    """Write facts about identity_group mapping."""
    if _seed is not None:
        random.seed(time.time())

    with open(persistent_ids_file) as f_h:
        pids = [data["pid"] for data in json_lines_file(f_h)]

    random.shuffle(pids)

    sizes, weights = zip(*[[k, v] for k, v in distribution.items()])
    i = 0
    with open(dst, "w") as f_h:
        while i < len(pids):
            size, *_ = random.choices(sizes, weights=weights)
            size = min(size, abs(len(pids) - i))
            persistent_ids = [pids[i + j] for j in range(size)]
            type_ = "household" if len(persistent_ids) < COMPANY_MIN_SIZE else "company"
            f_h.write(
                json.dumps(
                    {
                        "igid": hash_(persistent_ids),
                        "type": type_,
                        "persistentIds": persistent_ids,
                    }
                )
                + "\n"
            )
            # advance even if size was 0, meaning that persistent id
            # does not belong to any identity_group
            i += size or 1


def parse_distribution(size, weights):
    """Parse and validate distribution params."""
    if len(size) != len(weights):
        raise ValueError(
            "Identity group parsing issue: weights list and identity group "
            "size list are of different length"
        )

    eps = 1e-4
    # accept small errors, as floating point arithmetic cannot be done precisely on computers
    if not 1 - eps < sum(weights) < 1 + eps:
        raise ValueError(
            "Identity group parsing issue: weights must sum to 1, "
            f"but sum to {sum(weights)} instead"
        )
    return dict(zip(size, weights))


def get_ip_addresses(cidr):
    """Get list of hosts within given network cidr."""
    network = ipaddress.ip_network(cidr)
    hosts = list(network.hosts())
    if not hosts:
        return [network.network_address]
    return hosts


def build_iploc_knowledge(
    ip_facts_file,
    persistent_ids_facts_file,
    identity_group_facts_file,
    transient_ids_facts_file,
    dst,
):
    """
    Given some fact files, generate random locations and IP addresses in a sane way.

    It’s like funnel. At the very top you have identity groups, then persistent nodes,
    then transient nodes.

    Logic can be simplified to:
        * identity groups = select few (at most 8 with very low probability) IP addresses
        * persistent nodes = select few IP addresses from the group above
        * transient nodes = select few IP addresses from the group above

    This way context data is sane. Each transient node has IPs from subset of persistent
    id IPs, and identity groups IPs.

    Probabilities makes highly probably for transient nodes to be within the same city,
    and the same state. Same goes for persistent nodes.
    """
    IPLoc = namedtuple("IPLoc", "state, city, ip_address")

    with open(ip_facts_file) as f_h:
        ip_cidrs_by_state_city = list(json_lines_file(f_h))

    knowledge = {"identity_group": {}, "persistent_id": {}, "transient_ids": {}}

    def random_ip_loc():
        state_count, *_ = random.choices([1, 2], weights=[0.98, 0.02])
        for state_data in random.choices(ip_cidrs_by_state_city, k=state_count):
            city_count, *_ = random.choices(
                [1, 2, 3, 4], weights=[0.85, 0.1, 0.04, 0.01]
            )
            for city_data in random.choices(state_data["cities"], k=city_count):
                random_cidr = random.choice(city_data["cidr_blocks"])
                yield IPLoc(
                    state=state_data["state"],
                    city=city_data["city"],
                    ip_address=str(random.choice(get_ip_addresses(random_cidr))),
                )

    def random_ip_loc_from_group(locations):
        # compute weights, each next item is two times less likely probably than the previous
        weights = [1]
        for _ in locations[:-1]:
            weights.append(weights[-1] / 2)

        count = len(locations)
        random_count, *_ = random.choices(list(range(1, count + 1)), weights=weights)
        return list(set(random.choices(locations, k=random_count)))

    logger.info("Creating Identity group / persistent ids IP facts")
    with open(identity_group_facts_file) as f_h:
        for data in json_lines_file(f_h):
            locations = knowledge["identity_group"][data["igid"]] = list(
                set(random_ip_loc())
            )

            for persistent_id in data["persistentIds"]:
                knowledge["persistent_id"][persistent_id] = random_ip_loc_from_group(
                    locations
                )

    logger.info("Creating persistent / transient ids IP facts")
    with open(persistent_ids_facts_file) as f_h:
        for data in json_lines_file(f_h):
            persistent_id = data["pid"]
            # handle case where persistent id does not belong to any identity group
            if data["pid"] not in knowledge:
                knowledge["persistent_id"][persistent_id] = random_ip_loc_from_group(
                    list(set(random_ip_loc()))
                )
            for transient_id in data["transientIds"]:
                knowledge["transient_ids"][transient_id] = random_ip_loc_from_group(
                    knowledge["persistent_id"][persistent_id]
                )
        # now assign random ip location for transient ids without persistent ids
        logger.info("Processing remaining transient ids facts")
        with open(transient_ids_facts_file) as t_f_h:
            for data in json_lines_file(t_f_h):
                if data["uid"] not in knowledge["transient_ids"]:
                    knowledge["transient_ids"][data["uid"]] = list(
                        set(
                            random_ip_loc_from_group(  # "transient group" level
                                random_ip_loc_from_group(  # "persistent group" level
                                    list(set(random_ip_loc()))  # "identity group" level
                                )
                            )
                        )
                    )

    with open(dst, "w") as f_h:
        for key, data in knowledge["transient_ids"].items():
            f_h.write(
                json.dumps(
                    {"transient_id": key, "loc": [item._asdict() for item in data]}
                )
                + "\n"
            )

def generate_website_groups(urls_file, iab_categories, dst):
    """Generate website groups."""
    website_groups = {}
    with open(urls_file) as urls_f:
        urls_reader = csv.reader(urls_f, delimiter=",")
        for row in urls_reader:
            url = row[1]
            root_url = urlparse("//" + url).hostname
            if root_url not in website_groups:
                iab_category = random.choice(iab_categories)
                website_groups[root_url] = {
                    "websites": [url],
                    "category": {
                        "code": iab_category[0],
                        "name": iab_category[1]
                    }
                }
            else:
                website_groups[root_url]["websites"].append(url)

    with open(dst, "w") as dst_file:
        for url, data in website_groups.items():
            website_group = {
                "url": url,
                "websites": data["websites"],
                "category": data["category"]
            }
            website_group_id = hash_(website_group.items())
            website_group["id"] = website_group_id
            dst_file.write(
                json.dumps(website_group) + "\n"
            )


def read_iab_categories(iab_filepath):
    """Read IAB categories tuples from JSON file."""
    with open(iab_filepath) as iab_file:
        categories = json.loads(iab_file.read())
        return [(code, category) for code, category in categories.items()]


def build_user_identitity_knowledge(
    persistent_ids_facts_file, transient_ids_facts_file, dst
):
    """
    Generate some facts about user identities.

    There are few informations generated here:
        * transient ids types: cookie | device
        * transient id emails (it's randomly selected from persistent id emails)
        * transient id user agent (
            if transient id type is cookie then workstation user agent is generated,
            otherwise mobile one
        )
        * derivatives of user agent
            * device family (if type device)
            * OS
            * browser
    """
    user_emails = {}
    fake = Faker()
    fake.add_provider(UserAgentProvider)

    logger.info("Creating emails per transient ids")
    # create fake emails for devices with persistent ids
    with open(persistent_ids_facts_file) as f_h:
        for data in json_lines_file(f_h):
            nemail = random.randint(1, len(data["transientIds"]))
            emails = [fake.email() for _ in range(nemail)]
            for transient_id in data["transientIds"]:
                user_emails[transient_id] = random.choice(emails)

    # create fake emails for devices without persistent ids
    with open(transient_ids_facts_file) as t_f_h:
        for data in json_lines_file(t_f_h):
            if data["uid"] not in user_emails:
                user_emails[data["uid"]] = fake.email()

    logger.info("Writing down user identity facts")
    with open(dst, "w") as f_h:
        for transient_id, data in user_emails.items():
            type_ = random.choice(["cookie", "device"])
            uset_agent_str = fake.user_agent_from_type(type_)

            user_agent = parse(uset_agent_str)
            device = user_agent.device.family
            operating_system = user_agent.os.family
            browser = user_agent.browser.family

            f_h.write(
                json.dumps(
                    {
                        "transient_id": transient_id,
                        "user_agent": uset_agent_str,
                        "device": device,
                        "os": operating_system,
                        "browser": browser,
                        "email": data,
                        "type": type_,
                    }
                )
                + "\n"
            )


def register(parser):
    """Register 'add' parser."""
    add_parser = parser.add_parser("add")
    add_parser.add_argument("--config-file", type=argparse.FileType("r"), required=True)

    add_subparser = add_parser.add_subparsers()

    persistent_id_parser = add_subparser.add_parser("persistent_id")
    persistent_id_parser.set_defaults(subparser="add", command="persistent_id")

    identity_group_parser = add_subparser.add_parser("identity_group")
    identity_group_parser.add_argument("--size", type=int, dest="size", action="append")
    identity_group_parser.add_argument(
        "--weights", type=float, dest="weights", action="append"
    )
    identity_group_parser.set_defaults(subparser="add", command="identity_group")

    fact_parser = add_subparser.add_parser("fact")
    fact_parser.set_defaults(subparser="add", command="facts")

    website_groups_parser = add_subparser.add_parser("website_groups")
    website_groups_parser.set_defaults(subparser="add", command="website_groups")


def main(args):
    """Generate dataset files with information about the world."""
    config = configparser.ConfigParser()
    config.read(args.config_file.name)

    if args.command == "persistent_id":
        logger.info("Generate persistent id file to %s", config["dst"]["persistent"])
        uf_ds = extract_user_groups(config["src"]["user_to_user"])
        generate_persistent_groups(uf_ds, config["dst"]["persistent"])

    if args.command == "identity_group":
        logger.info(
            "Generate identity group file to %s", config["dst"]["identity_group"]
        )
        try:
            distribution = parse_distribution(args.size, args.weights)
        except ValueError as exc:
            print(exc)
            sys.exit(2)

        generate_identity_groups(
            config["dst"]["persistent"], distribution, config["dst"]["identity_group"]
        )

    if args.command == "facts":
        logger.info("Generate IP facts file to %s", config["dst"]["ip_info"])
        build_iploc_knowledge(
            ip_facts_file=config["src"]["location_to_cidr"],
            persistent_ids_facts_file=config["dst"]["persistent"],
            identity_group_facts_file=config["dst"]["identity_group"],
            transient_ids_facts_file=config["src"]["facts"],
            dst=config["dst"]["ip_info"],
        )
        logger.info(
            "Generate user identity facts file to %s",
            config["dst"]["user_identity_info"],
        )
        build_user_identitity_knowledge(
            persistent_ids_facts_file=config["dst"]["persistent"],
            transient_ids_facts_file=config["src"]["facts"],
            dst=config["dst"]["user_identity_info"],
        )

    if args.command == "website_groups":
        logger.info("Generate website groups file to %s.", config["dst"]["website_groups"])
        urls_file = config["src"]["urls"]
        dst_file = config["dst"]["website_groups"]
        iab_categories = read_iab_categories(config["src"]["iab_categories"])

        generate_website_groups(urls_file, iab_categories, dst_file)

    logger.info("Done!")
