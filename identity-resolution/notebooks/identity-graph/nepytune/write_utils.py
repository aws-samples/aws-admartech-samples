import abc
import csv
from contextlib import contextmanager
import json


class GremlinCSV:
    """Build CSV file in AWS-Neptune ready-to-load data format."""

    def __init__(self, opened_file, attributes):
        """Create CSV writer."""
        self.types = dict(key.split(":") for key in attributes)
        self.writer = csv.writer(opened_file, quoting=csv.QUOTE_ALL)
        self.key_order = list(self.types.keys())
        self.writer.writerow(self.header)

    def attributes(self, attribute_map):
        """Build attribute list from attribute_map with default values."""
        return [attribute_map.get(k, "") for k in self.key_order]

    @property
    @abc.abstractmethod
    def header(self):
        """Get header."""


class GremlinNodeCSV(GremlinCSV):
    """Build CSV file with graph nodes in AWS-Neptune ready-to-load data format."""

    @property
    def header(self):
        """Get header."""
        return (
            ["~id"]
            + [f"{key}:{self.types[key]}" for key in self.key_order]
            + ["~label"]
        )

    def add(self, _id, attribute_map, label):
        """Add row to CSV file."""
        self.writer.writerow([_id] + self.attributes(attribute_map) + [label])


class GremlinEdgeCSV(GremlinCSV):
    """Build CSV file with graph edges in AWS-Neptune ready-to-load data format."""

    @property
    def header(self):
        """Get header."""
        return ["~id", "~from", "~to", "~label"] + [
            f"{key}:{self.types[key]}" for key in self.key_order
        ]

    def add(self, _id, _from, to, label, attribute_map):
        """Add row to CSV file."""
        self.writer.writerow([_id, _from, to, label] + self.attributes(attribute_map))


@contextmanager
def gremlin_writer(type_, file_name, attributes):
    """Factory of gremlin writer objects."""
    with open(file_name, "w", 1024 * 1024) as f_t:
        yield type_(f_t, attributes=attributes)


def json_lines_file(opened_file):
    """Yield json lines from opened file."""
    for line in opened_file:
        yield json.loads(line)
