from collections import namedtuple

from nepytune.write_utils import gremlin_writer, GremlinNodeCSV, json_lines_file
from nepytune.utils import hash_


IPLoc = namedtuple("IPLoc", "state, city, ip_address")


def get_id(ip_loc):
    """Generate id from ip loc."""
    return hash_([ip_loc.state, ip_loc.city, ip_loc.ip_address])


def generate_ip_loc_nodes_from_facts(src, dst):
    """Generate ip location csv file with nodes."""
    attrs = ["state:String", "city:String", "ip_address:String"]
    with open(src) as f_h:
        with gremlin_writer(GremlinNodeCSV, dst, attributes=attrs) as writer:
            locations = set()
            for data in json_lines_file(f_h):
                for fact in data["facts"]:
                    locations.add(
                        IPLoc(fact["state"], fact["city"], fact["ip_address"])
                    )

            for location in locations:
                writer.add(
                    _id=get_id(location),
                    attribute_map={
                        "state": location.state,
                        "city": location.city,
                        "ip_address": location.ip_address,
                    },
                    label="IP",
                )
