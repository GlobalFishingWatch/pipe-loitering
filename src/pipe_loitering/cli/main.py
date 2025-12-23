#! /usr/bin/env python
import sys
import logging

from pipe_loitering.create_raw_loitering import run_create_raw_loitering
from pipe_loitering.merge_raw_loitering import run_merge_raw_loitering

logging.basicConfig(level=logging.INFO)

SUBCOMMANDS = {
    "create_raw_loitering": run_create_raw_loitering,
    "merge_raw_loitering": run_merge_raw_loitering,
}


def main():
    logging.info("Running %s", sys.argv)

    if len(sys.argv) < 2:
        logging.info("No subcommand specified. Run pipeline [SUBCOMMAND], where subcommand is one of %s", SUBCOMMANDS.keys())
        exit(1)

    subcommand = sys.argv[1]
    subcommand_args = sys.argv[2:]

    SUBCOMMANDS[subcommand](subcommand_args)


if __name__ == "__main__":
    main()
