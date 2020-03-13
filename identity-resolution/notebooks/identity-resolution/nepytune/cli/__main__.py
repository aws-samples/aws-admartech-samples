import argparse
import logging

from nepytune.cli.transform import (
    register as transform_register,
    main as transform_main,
)
from nepytune.cli.split import register as split_register, main as split_main
from nepytune.cli.add import register as add_register, main as add_main
from nepytune.cli.extend import register as extend_register, main as extend_main


logging.basicConfig(format="%(asctime)-15s %(message)s")


def main():
    """Main entry point for all commands."""
    parser = argparse.ArgumentParser(description="Extend/generate dataset csv files")
    parser.set_defaults(subparser="none")

    subparsers = parser.add_subparsers()

    transform_register(subparsers)
    split_register(subparsers)
    add_register(subparsers)
    extend_register(subparsers)

    args = parser.parse_args()

    if args.subparser == "transform":
        transform_main(args)

    if args.subparser == "split":
        split_main(args)

    if args.subparser == "add":
        add_main(args)

    if args.subparser == "extend":
        extend_main(args)


if __name__ == "__main__":
    main()
