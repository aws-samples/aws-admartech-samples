from nepytune.write_utils import gremlin_writer, GremlinNodeCSV, json_lines_file


def generate_identity_group_nodes(src, dst):
    """Generate identity_group csv file with nodes."""
    attrs = ["igid:String", "type:String"]
    with open(src) as f_h:
        with gremlin_writer(GremlinNodeCSV, dst, attributes=attrs) as writer:
            for data in json_lines_file(f_h):
                if data["persistentIds"]:
                    writer.add(
                        _id=data["igid"],
                        attribute_map={"igid": data["igid"], "type": data["type"]},
                        label="identityGroup",
                    )
