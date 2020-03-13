from nepytune.write_utils import gremlin_writer, GremlinEdgeCSV, json_lines_file
from nepytune.utils import get_id


def generate_persistent_id_edges(src, dst):
    """Generate persistentID edges based on union-find datastructure."""
    with open(src) as f_h:
        with gremlin_writer(GremlinEdgeCSV, dst, attributes=[]) as writer:
            for data in json_lines_file(f_h):
                for node in data["transientIds"]:
                    persistent_to_transient = {
                        "_id": get_id(data["pid"], node, {}),
                        "_from": data["pid"],
                        "to": node,
                        "label": "has_identity",
                        "attribute_map": {},
                    }
                    writer.add(**persistent_to_transient)
