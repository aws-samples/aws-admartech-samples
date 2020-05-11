from nepytune.utils import get_id
from nepytune.write_utils import gremlin_writer, GremlinEdgeCSV, json_lines_file


WEBISTE_GROUP_EDGE_LABEL = "links_to"


def generate_website_group_edges(website_group_json, dst):
    """Generate website group edges CSV."""
    with open(website_group_json) as f_h:
        with gremlin_writer(GremlinEdgeCSV, dst, attributes=[]) as writer:
            for data in json_lines_file(f_h):
                root_id = data["id"]
                websites = data["websites"]
                for website in websites:
                    writer.add(
                        _id=get_id(root_id, website, {}),
                        _from=root_id,
                        to=website,
                        label=WEBISTE_GROUP_EDGE_LABEL,
                        attribute_map={}
                    )
