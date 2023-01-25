from nepytune.utils import get_id
from nepytune.write_utils import gremlin_writer, GremlinEdgeCSV, json_lines_file


#WEBISTE_GROUP_EDGE_LABEL = "passenger_booking"


def generate_website_group_edges(website_group_json, dst):
    """Generate website group edges CSV."""
    with open(website_group_json) as f_h:
        with gremlin_writer(GremlinEdgeCSV, dst, attributes=[]) as writer:
            for data in json_lines_file(f_h):
                root_id = data["booking_id"]
                person_ids = data["person_id"]
                for person_id in person_ids:
                    writer.add(
                        _id=get_id(root_id, person_id, {}),
                        _from=root_id,
                        to=person_ids,
                        label='passenger_booking',
                        attribute_map={}
                    )
