from nepytune.nodes.ip_loc import IPLoc, get_id
from nepytune.write_utils import gremlin_writer, GremlinEdgeCSV, json_lines_file
from nepytune.utils import get_id as get_edge_id


def generate_ip_loc_edges_from_facts(src, dst):
    """Generate ip location csv file with edges."""
    with open(src) as f_h:
        with gremlin_writer(GremlinEdgeCSV, dst, attributes=[]) as writer:
            for data in json_lines_file(f_h):
                uid_locations = set()
                for fact in data["facts"]:
                    uid_locations.add(
                        IPLoc(fact["state"], fact["city"], fact["ip_address"])
                    )

                for location in uid_locations:
                    loc_id = get_id(location)
                    writer.add(
                        _id=get_edge_id(data["uid"], loc_id, {}),
                        _from=data["uid"],
                        to=loc_id,
                        label="uses",
                        attribute_map={},
                    )
