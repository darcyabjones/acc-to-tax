#! /usr/bin/env python3

from __future__ import print_function

import os
from os.path import split as psplit
from os.path import splitext as splitext
import re
import argparse
import sys
from collections import defaultdict

program = "acc2tax-db"
version = "0.1.0"
author = "Darcy Jones"
date = "22 January 2018"
email = "darcy.ab.jones@gmail.com"
short_blurb = (
    'Constructs a SQL database from NCBI taxonomy files.'
    )
license = (
    '{program}-{version}\n'
    '{short_blurb}\n\n'
    'Copyright (C) {date},  {author}'
    '\n\n'
    'This program is free software: you can redistribute it and/or modify '
    'it under the terms of the GNU General Public License as published by '
    'the Free Software Foundation, either version 3 of the License, or '
    '(at your option) any later version.'
    '\n\n'
    'This program is distributed in the hope that it will be useful, '
    'but WITHOUT ANY WARRANTY; without even the implied warranty of '
    'MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the '
    'GNU General Public License for more details.'
    '\n\n'
    'You should have received a copy of the GNU General Public License '
    'along with this program. If not, see <http://www.gnu.org/licenses/>.'
    )

license = license.format(**locals())


"################################# Classes ##################################"


class Acc2Tax(Base):
    __tablename__ = 'acc2tax'
    acc_ver = Column(String, primary_key=True)
    acc = Column(String)
    taxid = Column(Integer)
    gi = Column(Integer)


class TaxCat(Base):
    __tablename__ = 'taxcat'
    category = Column(String)
    category_taxid = Column(Integer)
    species_taxid = Column(Integer, primary_key=True)


class Nodes(Base):
    """
    Table schema to hold "nodes.dmp" from taxdump folder.

    Description of file columns in NCBI ftp:
    tax_id -- node id in GenBank taxonomy database
    parent tax_id -- parent node id in GenBank taxonomy database
    rank -- rank of this node (superkingdom, kingdom, ...) 
    embl code -- locus-name prefix; not unique
    division id -- see division.dmp file
    inherited div flag  (1 or 0) -- 1 if node inherits division from parent
    genetic code id -- see gencode.dmp file
    inherited GC  flag  (1 or 0) -- 1 if node inherits genetic code from parent
    mitochondrial genetic code id -- see gencode.dmp file
    inherited MGC flag  (1 or 0) -- 1 if node inherits mitochondrial gencode from parent
    GenBank hidden flag (1 or 0) -- 1 if name is suppressed in GenBank entry lineage
    hidden subtree root flag (1 or 0) -- 1 if this subtree has no sequence data yet
    comments -- free-text comments and citations
    """
    __tablename__ = 'nodes'

    taxid = Column(Integer, primary_key=True) # 
    parent_taxid = Column(Integer)
    rank = Column(String)
    embl_code = Column(String)
    division_id = Column(String) # Foreign key
    inherited_div_flag = Column(Boolean)
    genetic_code_id = Column(String) # Foreign key
    inherited_gc_flag = Column(Boolean)
    mitochonchondrial_genetic_code_id = Column(String) # Foreign key, poss same as genetic code id.
    inherited_mgc_flag = Column(Boolean)
    genbank_hidden_flag = Column(Boolean)
    hidden_subtree_root_flag = Column(Boolean)
    comment = Column(String)


class Names(Base):
    """ Table schema for "names.dmp" from taxdmp folder

    Description of file columns:
    tax_id -- the id of node associated with this name
    name_txt -- name itself
    unique name -- the unique variant of this name if name not unique
    name class -- (synonym, common name, ...)
    """
    __tablename__ = 'names'

    taxid = Column(Integer) # Foreign key link to nodes
    name = Column(String)
    unique_name = Column(String)
    name_class = Column(String)


class Division(Base):
    """ Table schema for "divisions.dmp" from taxdmp folder

    Description of file columns:
    division id -- taxonomy database division id
    division cde -- GenBank division code (three characters)
    division name -- e.g. BCT, PLN, VRT, MAM, PRI...
    comments
    """

    __tablename__ = "division"

    division_id = Column(String) # Foreign key with nodes.
    division_cde = Column(String)
    division_name = Column(String)
    comments = Column(String)


class GenCode(Base):
    """ Table schema for "gencode.dmp" from taxdmp folder.

    Description of file columns:

    genetic code id -- GenBank genetic code id
    abbreviation -- genetic code name abbreviation
    name -- genetic code name
    cde -- translation table for this genetic code
    starts -- start codons for this genetic code
    """

    __tablename__ = "gencode"

    code_id = Column(String, primary_key=True)
    abbreviation = Column(String)
    name = Column(String)
    cde = Column(String)
    starts = Column(String)


class DelNodes(Base):
    """ Deleted nodes.
    Not sure If I want to support updating the database or if I should just
    force reconstruction.

    tax_id -- deleted node id
    """

    __tablename__ = "delnodes"
    taxid = Column(String, primary_key=True)


class Merged(Base):
    """ "merged.dmp" 

    Merged nodes file fields:
    old_tax_id -- id of nodes which has been merged
    new_tax_id -- id of nodes which is result of merging
    """

    __tablename__ = "merged"
    old_taxid = Column(Integer)
    new_taxid = Column(Integer)


class Citations(Base):
    """ "citations.dmp"

    Citations file fields:
    cit_id -- the unique id of citation
    cit_key -- citation key
    pubmed_id -- unique id in PubMed database (0 if not in PubMed)
    medline_id -- unique id in MedLine database (0 if not in MedLine)
    url -- URL associated with citation
    text -- any text (usually article name and authors)
         -- The following characters are escaped in this text by a backslash:
         -- newline (appear as "\n"),
         -- tab character ("\t"),
         -- double quotes ('\"'),
         -- backslash character ("\\").
    taxid_list-- list of node ids separated by a single space
    """

    __tablename__ = "citations"
    cit_id = Column(String)
    cit_key = Column(String)
    pubmed_id = Column(Integer)
    medline_id = Column(Integer)
    url = Column(String)
    text = Column(String)
    taxid_list = Column(String)


def main(db, acc2taxid=None, taxcat=None, taxdump=None):
    """ Creates a database to access. """

    return



"########################### Argument Handling ##############################"

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=license,
        )
    arg_parser.add_argument(
        "-i", "--infile",
        default=sys.stdin,
        type=argparse.FileType('r'),
        help=(
            "Path to the input file."
            "Default is stdin.\n"
            "Note that only one of infile and gff can take input from stdin."
            ),
        )
    arg_parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s {}'.format(version),
        )

    args = arg_parser.parse_args()

    main(**args.__dict__)
