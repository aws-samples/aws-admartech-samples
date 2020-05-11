from nepytune.write_utils import gremlin_writer, GremlinEdgeCSV, json_lines_file
from nepytune.utils import get_id


def generate_identity_group_edges(src, dst):
    """Generate identity_group edge csv file."""
    with open(src) as f_h:
        with gremlin_writer(GremlinEdgeCSV, dst, attributes=[]) as writer:
            for data in json_lines_file(f_h):
                persistent_ids = data["persistentIds"]
                if persistent_ids:
                    for persistent_id in persistent_ids:
                        identity_group_to_persistent = {
                            "_id": get_id(data["igid"], persistent_id, {}),
                            "_from": data["igid"],
                            "to": persistent_id,
                            "attribute_map": {},
                            "label": "member",
                        }
                        writer.add(**identity_group_to_persistent)
