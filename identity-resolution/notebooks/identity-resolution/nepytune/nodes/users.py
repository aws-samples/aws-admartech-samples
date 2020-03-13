from nepytune.write_utils import gremlin_writer, json_lines_file, GremlinNodeCSV


def generate_user_nodes(src, dst):
    """Generate user node csv file."""
    attributes = [
        "uid:String",
        "user_agent:String",
        "device:String",
        "os:String",
        "browser:String",
        "email:String",
        "type:String",
    ]
    with open(src) as src_data:
        with gremlin_writer(GremlinNodeCSV, dst, attributes=attributes) as writer:
            for data in json_lines_file(src_data):
                writer.add(
                    _id=data["uid"],
                    attribute_map={
                        "uid": data["uid"],
                        "user_agent": data["user_agent"],
                        "device": data["device"],
                        "os": data["os"],
                        "browser": data["browser"],
                        "email": data["email"],
                        "type": data["type"],
                    },
                    label="transientId",
                )
        return dst


def generate_persistent_nodes(src, dst):
    """Generate persistent node csv file."""
    with open(src) as f_h:
        with gremlin_writer(GremlinNodeCSV, dst, attributes=["pid:String"]) as writer:
            for data in json_lines_file(f_h):
                writer.add(
                    _id=data["pid"],
                    attribute_map={"pid": data["pid"]},
                    label="persistentId",
                )
