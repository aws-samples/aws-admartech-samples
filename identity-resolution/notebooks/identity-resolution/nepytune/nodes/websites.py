import csv
import collections

from nepytune.utils import hash_
from nepytune.write_utils import gremlin_writer, GremlinNodeCSV, json_lines_file

WEBSITE_LABEL = "website"
WEBSITE_GROUP_LABEL = "websiteGroup"

Website = collections.namedtuple("Website", ["url", "title"])


def generate_website_nodes(urls, titles, dst):
    """
    Generate Website nodes and save it into csv file.

    The CSV is compatible with AWS Neptune Gremlin data format.

    Website nodes are generated from dataset files:
        * urls.csv
        * titles.csv

    Files contain maps of fact_id and website url/title.
    Data is joined by fact_id.
    """

    urls = read_urls_from_csv(urls)
    titles = read_titles_from_csv(titles)
    generate_website_csv(urls, titles, dst)


def generate_website_group_nodes(website_group_json, dst):
    """Generate website groups csv."""
    attributes = [
        "url:String",
        "category:String",
        "categoryCode:String"
    ]
    with open(website_group_json) as f_h:
        with gremlin_writer(GremlinNodeCSV, dst, attributes=attributes) as writer:
            for data in json_lines_file(f_h):
                writer.add(
                    _id=data["id"],
                    attribute_map={
                        "url": data["url"],
                        "category": data["category"]["name"],
                        "categoryCode": data["category"]["code"]
                    },
                    label=WEBSITE_GROUP_LABEL
                )


def read_urls_from_csv(path):
    """Return dict with urls and fact ids corresponding to them."""
    urls = collections.defaultdict(list)
    with open(path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=",")
        for row in csv_reader:
            fid = row[0]
            url = row[1]
            urls[url].append(fid)
    return urls


def read_titles_from_csv(path):
    """Read titles from csv."""
    titles = {}
    with open(path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=",")
        for row in csv_reader:
            fid = row[0]
            title = row[1]
            titles[fid] = title
    return titles


def generate_websites(urls, titles):
    """Yield rows in CSV format."""
    for url, fids in urls.items():
        title = get_website_title(fids, titles)
        yield Website(url, title)


def get_website_title(fids, titles):
    """Get website title."""
    for fid in fids:
        title = titles.get(fid)
        if title:
            return title
    return None


def generate_website_csv(urls, titles, dst):
    """Generate destination CSV file."""
    attributes = ["url:String", "title:String"]
    with gremlin_writer(GremlinNodeCSV, dst, attributes=attributes) as writer:
        for website in generate_websites(urls, titles):
            attribute_map = {"url": website.url, "title": website.title}
            writer.add(
                _id=website.url, attribute_map=attribute_map, label=WEBSITE_LABEL
            )
