import json
import csv
import argparse


def batch_facts(src, size):
    """Split facts into batches of provided size."""
    with open(src) as f_h:
        json_lines = []
        i = 0

        for line in f_h:
            if i > size:
                yield json_lines
                i = 0
                json_lines = []

            json_lines.append(json.loads(line))
            i = i + 1

        yield json_lines


def write_json_facts(json_lines, dst):
    """Write down jsonline facts into dst."""
    with open(dst, "w") as f_h:
        for data in json_lines:
            f_h.write(json.dumps(data) + "\n")


def load_urls(src):
    """
    Load given url file csv into memory.

    It assumes that only two columns are present. One is key, other is value.
    """
    with open(src) as f_h:
        data = csv.reader(f_h, delimiter=",")
        return dict((int(row[0]), row[1]) for row in data)


def write_urls(json_facts, urls, dst):
    """Write down urls batch based on batch of json facts."""
    with open(dst, "w") as f_h:
        writer = csv.writer(f_h, delimiter=",")
        for data in json_facts:
            for fact in data["facts"]:
                writer.writerow([fact["fid"], urls[fact["fid"]]])


def register(parser):
    """Register 'split' command."""
    split_parser = parser.add_parser("split")
    split_parser.set_defaults(subparser="split")

    split_parser.add_argument("--size", type=int, required=True)
    split_parser.add_argument(
        "--facts-file", type=argparse.FileType("r"), required=True
    )
    split_parser.add_argument("--urls-file", type=argparse.FileType("r"), required=True)
    split_parser.add_argument("--dst-folder", type=str, required=True)


def main(args):
    """'Split' command logic."""
    location, size = args.dst_folder, args.size
    urls = load_urls(args.urls_file.name)
    i = 0
    file_prefix = f"{i * size}_{(i + 1) * size}"
    for json_lines in batch_facts(args.facts_file.name, size):
        i = i + 1
        write_json_facts(json_lines, dst=f"{location}/{file_prefix}_facts.json")
        write_urls(json_lines, urls, dst=f"{location}/{file_prefix}_urls.csv")
        file_prefix = f"{i * size}_{(i + 1)* size}"
