"""
Get new taxids.
"""
from __future__ import unicode_literals

version = "0.0.1"


import sys
import re
import logging
import argparse
from collections import defaultdict
from collections import Iterable
from contextlib import contextmanager

from sqlalchemy import create_engine

from .database import session_scope
from .database import Base
from .database import Nodes
from .database import Names
from .database import Division


logger = logging.getLogger(__name__)
logger.debug("Loaded module: `acclist`")

def get_taxids(engine, taxids, children=True, parents=False):
    with session_scope(engine) as session:
        taxids = Nodes.get_taxids(taxids, session=session)
        if children:
            child_taxids = Nodes.get_children(taxids, session=session)
        else:
            child_taxids = []

        if parents:
            parent_taxids = Nodes.get_parents(taxids, session=session)
        else:
            parent_taxids = []

        output = [n.taxid for n in ( taxids + child_taxids + parent_taxids )]

    return output

def filter_acc(fpaths, taxids, inverse=False):
    sep = "\t"
    columns = ("accession", "accession_version", "taxid", "gi")

    header=None
    for fpath in fpaths:
        with open(fpath) as handle:
            for line in handle:
                sline = line.strip().split(sep)
                if header is None:
                    header = sline
                    continue

                dline = dict(zip(header, sline))
                target_match = (int(dline["taxid"]) in taxids)

                if ((not target_match and inverse) or 
                        (target_match and not inverse)):
                    print(dline["accession"])

    return

def main(args=sys.argv):
    arg_parser = argparse.ArgumentParser(
        description="test",
        epilog=(
            'Example usage:\n'
            )
        )
    arg_parser.add_argument(
        "-d", "--db",
        default="sqlite:///db.sqlite",
        help="The address to the database to write to."
        )

    arg_parser.add_argument(
        "-t", "--taxids",
        type=int,
        nargs='+',
        help=(
            "Taxids to get."
            )
        )
    arg_parser.add_argument(
        "-f", "--fpaths",
        nargs='+',
        help=(
            "Taxids to get."
            )
        )
    arg_parser.add_argument(
        "-c", "--no-children",
        action="store_false",
        default=True,
        help=(
            "Taxids to get."
            )
        )
    arg_parser.add_argument(
        "-p", "--parents",
        action="store_true",
        default=False,
        help=(
            "Taxids to get."
            )
        )
    arg_parser.add_argument(
        "--debug",
        default=False,
        action='store_true',
        help='Display debug information about compiled expression.',
        )
    arg_parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s {}'.format(version),
        )


    args = arg_parser.parse_args()

    engine = create_engine(args.db)
    x = get_taxids(engine, taxids=args.taxids, children=args.no_children)
    print("\n".join([str(i) for i in x]))
    #print(x)
    #filter_acc(args.fpaths, x)
    return

