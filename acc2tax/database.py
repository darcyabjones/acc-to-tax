"""
Templates to store NCBI taxonomy files in sql database.
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

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import declared_attr

from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy import ForeignKey

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import and_

Base = declarative_base()

logger = logging.getLogger(__name__)
logger.debug("Loaded module: `database`")


@contextmanager
def session_scope(engine):
    """Provide a transactional scope around a series of operations."""
    session = sessionmaker(engine)()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

def int2bool(x):
    """ Converts integer strings to boolean objects. """
    return bool(int(x))

def bool2int(x):
    """ Converts boolean values back to integers in string objects. """
    return str(int(x))

class BaseTable(object):

    """
    Base class for NCBI taxonomy SQLalchemy tables.
    Intended to be subclassed, will not work on it's own.

    Defines several class methods for working with the SQLAlchemy ORM.
    """

    """
    Columns class variable is used for reading from txt files.
    Can be changed when invoking the class method
    The columns are provided as tuples:
        [0]=name
        [1]=function to process string into appropriate python object
        [2]=function to transform python object back for printing.

    This attribute should be overwritten in subclasses.
    """
    columns = []

    """
    The column delimiters for reading the text file.
    Again, should be overwritted in subclasses.
    """
    sep = "\t"
    end = "\n"
    header = False

    """
    The maximum number of rows to search for at a time.
    """
    max_search_rows = 900

    # Always name the table as the class name in lower case.
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    # Always have dedicated primary key
    id = Column(Integer, primary_key=True)

    @classmethod
    def get_session(cls, session=None):
        """ Get the sqlalchemy session or fail gracefully. """
        if session is None:
            try:
                session = cls.session
            except AttributeError:
                raise ValueError("A session was not provided "
                                 "to the method or object.")
        return session

    @staticmethod
    def line_trans(line, sep, end, columns):
        output = {}
        sline = re.sub(re.escape(end) + "$", '', line).split(sep)
        for column, (colname, trans, rev_trans) in zip(sline, columns):
            output[colname] = trans(column)
        return output

    @staticmethod
    def string_fmt(record, sep, end, columns):
        output = []
        for colname, trans, rev_trans in columns:
            value = getattr(record, colname)
            if value is None:
                value = ""
            output.append(rev_trans(value))

        return sep.join(output) + end

    @classmethod
    def from_file(
            cls,
            filepath,
            session=None,
            sep=None,
            end=None,
            columns=None,
            header=None
            ):
        """ Read a file into the SQL taxonomy database. """

        session = cls.get_session(session)

        if columns is None:
            columns = cls.columns

        if sep is None:
            sep = cls.sep

        if end is None:
            end = cls.end

        if header is None:
            header = cls.header

        with open(filepath, "r") as handle:
            #if there is a header skip the first line.
            if header:
                next(handle)

            session.bulk_insert_mappings(
                cls,
                [cls.line_trans(l, sep, end, columns) for l in handle]
            )

        session.commit()
        return

    @classmethod
    def to_table(
            cls,
            records,
            sep="\t",
            end="",
            columns=None,
            header=None
            ):
        """ Write out the database or subset of as a string. """

        if columns is None:
            columns = cls.columns

        if header is None:
            header = cls.header

        if header:
            yield sep.join([c[0] for c in columns]) + end

        for record in records:
            yield cls.string_fmt(record, sep, end, columns)

    @classmethod
    def query(cls, columns=None, session=None):
        session = cls.get_session(session)

        if columns is None:
            return session.query(cls)

        if not isinstance(columns, Iterable) or isinstance(columns, str):
            columns = [columns]

        return session.query(*[getattr(cls, column) for column in columns])


    @classmethod
    def filter(cls, statement, columns=None, session=None, **kwargs):
        session = cls.get_session(session)
        return cls.query(columns=columns, session=session).filter(statement)

    @classmethod
    def get_records(cls, column, values, max_search_rows=None, session=None):

        session = cls.get_session(session)

        if max_search_rows is None:
            max_search_rows = cls.max_search_rows

        results = list()
        for i in range(0, len(values), max_search_rows):
            j = i + max_search_rows
            these_results = cls.filter(
                statement=getattr(cls, column).in_(values[i:j]),
                columns=None,
                session=session
                ).all()
            results.extend(these_results)

        return results

    @classmethod
    def get_record(cls, column, value, session=None):

        session = cls.get_session(session)

        result = cls.filter(
            statement=getattr(cls, column) == value,
            columns=None,
            session=session,
            ).one()

        return result

    @staticmethod
    def sanitise_integer(integer):
        """ Bit redundant but allows us to handle more cases in the future """
        return int(integer)

    @classmethod
    def sanitise_integers(cls, integers):
        if isinstance(integers, Iterable) and not isinstance(integers, str):
            return [cls.sanitise_integer(i) for i in integers]

        return [cls.sanitise_integer(integers)]

    @staticmethod
    def sanitise_string(string):
        """ Bit redundant but allows us to handle more cases in the future """
        return str(string)

    @classmethod
    def sanitise_strings(cls, strings):
        if isinstance(strings, Iterable) and not isinstance(strings, str):
            return [cls.sanitise_string(s) for s in strings]

        return [cls.sanitise_string(strings)]

    @classmethod
    def _separate_objs(cls, objs):
        obj = list()
        not_obj = list()

        if not isinstance(objs, Iterable) and not isinstance(objs, str):
            objs = [objs]

        for o in objs:
            if isinstance(o, (str, int, bool)):
                not_obj.append(o)
            else:
                obj.append(o)
        return obj, not_obj


    def __repr__(self):
        prefix = self.__class__.__name__

        args = list()
        for colname, ftrans, rtrans in self.columns:
            value = getattr(self, colname)

            # Add quotes around strings
            if ftrans == str:
                value = "'{}'".format(value)

            args.append("{}={}".format(colname, value))

        return "{}({})".format(prefix, ", ".join(args))


    def __str__(self):
        args = list()
        try:
            important_cols = self.important_cols
        except AttributeError:
            important_cols = [c[0] for c in self.columns]

        for colname in important_cols:
            value = getattr(self, colname)

            if colname == 'rank':
                value = "'{}'".format(value)

            args.append("{}: {}".format(colname, value))
        return ", ".join(args)



class Acc2Tax(BaseTable, Base):
    """ Table to hold any of the files mapping accession to taxid.

    Columns in the table are:
    accession
    accession.version
    taxid
    gi

    Note that these files may have a header column
    """

    accession = Column(String)
    accession_version = Column(String, unique=True)

    taxid = Column(Integer, ForeignKey('nodes.taxid'))
    node = relationship("Nodes", back_populates="accessions")

    gi = Column(Integer)

    """
    Columns class variable is used for reading from txt files.
    Can be changed when invoking the class method
    The columns are provided as tuples:
        [0]=name
        [1]=function to process string into appropriate python object
        [2]=function to transform python object back for printing.
    """
    columns = [
        ("accession", str, str),
        ("accession_version", str, str),
        ("taxid", int, str),
        ("gi", int, str),
        ]

    # The column delimiter for the text file.
    sep = "\t"
    end = "\n"
    header = True


class Nodes(BaseTable, Base):
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

    taxid = Column(Integer, unique=True)
    parent_taxid = Column(Integer)
    rank = Column(String)
    embl_code = Column(String)

    division_id = Column(Integer, ForeignKey("division.division_id")) # Foreign key
    division = relationship("Division", back_populates="nodes")

    inherited_div_flag = Column(Boolean)
    genetic_code_id = Column(String) # Foreign key
    inherited_genetic_code_flag = Column(Boolean)
    mitochonchondrial_genetic_code_id = Column(String) # Foreign key, poss same as genetic code id.
    inherited_mitochonchondrial_genetic_code_flag = Column(Boolean)
    genbank_hidden_flag = Column(Boolean)
    hidden_subtree_root_flag = Column(Boolean)
    comments = Column(String)

    names = relationship("Names", back_populates="node")
    accessions = relationship("Acc2Tax", back_populates="node")

    # Columns class variable is used for reading from txt files.
    # Can be changed when invoking the class method
    columns = [
        ("taxid", int, str),
        ("parent_taxid", int, str),
        ("rank", str, str),
        ("embl_code", str, str),
        ("division_id", int, str),
        ("inherited_div_flag", bool, bool2int),
        ("genetic_code_id", str, str),
        ("inherited_genetic_code_flag", bool, bool2int),
        ("mitochonchondrial_genetic_code_id", str, str),
        ("inherited_mitochonchondrial_genetic_code_flag", bool, bool2int),
        ("genbank_hidden_flag", bool, bool2int),
        ("hidden_subtree_root_flag", bool, bool2int),
        ("comments", str, str)
        ]

    # These are the columns displayed in __str__
    important_cols = ['taxid', 'parent_taxid', 'rank']

    # The file delimiter for the text file.
    sep = "\t|\t"
    end = "\t|\n"
    header = False

    @classmethod
    def get_taxids(cls, taxids, session=None):
        """ Finds a rows given a taxid """

        session = cls.get_session(session)
        taxids = cls.sanitise_integers(taxids)

        return cls.get_records("taxid", taxids, session=session)

    @classmethod
    def get_parents(cls, nodes, rank=None, session=None):
        """ Finds parents for a file based on, """

        session = cls.get_session(session)
        nodes, taxids = cls._separate_objs(nodes)

        nodes.extend(cls.get_taxids(taxids, session=session))
        parent_taxids = [n.parent_taxid for n in nodes]

        seen = set()

        def recurse(taxids, seen):
            taxids = [t for t in taxids if t not in seen]
            if len(taxids) == 0:
                return []

            records = cls.get_records("taxid", taxids, session=session)
            seen.update(taxids)

            parent_taxids = [r.parent_taxid for r in records]
            parent_records = recurse(parent_taxids, seen)

            records.extend(parent_records)
            return records

        return recurse(parent_taxids, seen)

    def parents(self, rank=None, session=None):
        return self.__class__.get_parents(nodes=self, rank=rank, session=session)

    @classmethod
    def get_children(cls, nodes, session=None):

        session = cls.get_session(session)
        nodes, taxids = cls._separate_objs(nodes)

        taxids.extend([n.taxid for n in nodes])

        seen = set()

        def recurse(taxids, seen):
            taxids = [t for t in taxids if t not in seen]
            if len(taxids) == 0:
                return []

            child_records = cls.get_records("parent_taxid", taxids, session=session)
            seen.update(taxids)

            child_taxids = [r.taxid for r in child_records]
            grandchild_records = recurse(child_taxids, seen)

            child_records.extend(grandchild_records)
            return child_records

        return recurse(taxids, seen)


class Names(BaseTable, Base):
    """ Table schema for "names.dmp" from taxdmp folder

    Description of file columns:
    tax_id -- the id of node associated with this name
    name_txt -- name itself
    unique name -- the unique variant of this name if name not unique
    name class -- (synonym, common name, ...)
    """

    taxid = Column(Integer, ForeignKey('nodes.taxid'))
    name = Column(String)
    unique_name = Column(String)
    name_class = Column(String)

    node = relationship("Nodes", back_populates="names")

    # Columns class variable is used for reading from txt files.
    # Can be changed when invoking the class method
    columns = [
        ("taxid", int, str),
        ("name", str, str),
        ("unique_name", str, str),
        ("name_class", str, str),
        ]

    # The file delimiter for the text file.
    sep = "\t|\t"
    end = "\t|\n"
    header = False

    @classmethod
    def get_taxids(cls, taxids, name_class=None, max_search_rows=None, session=None):
        """ Finds a rows given a taxid """

        session = cls.get_session(session)
        taxids = cls.sanitise_integers(taxids)
        if max_search_rows is None:
            max_search_rows = cls.max_search_rows

        if name_class is not None:
            name_class = cls.sanitise_strings(name_class)

        results = list()

        for i in range(0, len(taxids), max_search_rows):
            j = i + max_search_rows

            taxid_filter = cls.taxid.in_(taxids[i:j])

            if name_class is not None:
                nc_filter = cls.name_class.in_(name_class)

                total_filter = and_(taxid_filter, nc_filter)
            else:
                total_filter = taxid_filter

            these_results = cls.filter(
                statement=total_filter,
                columns=None,
                session=session,
                ).all()
            results.extend(these_results)

        return results


class Division(BaseTable, Base):
    """ Table schema for "divisions.dmp" from taxdmp folder

    Description of file columns:
    division id -- taxonomy database division id
    division cde -- GenBank division code (three characters)
    division name -- e.g. BCT, PLN, VRT, MAM, PRI...
    comments
    """

    division_id = Column(Integer, unique=True) # Foreign key with nodes.
    division_cde = Column(String) # Three letter name
    division_name = Column(String)
    comments = Column(String)

    nodes = relationship("Nodes", back_populates="division")

    # Columns class variable is used for reading from txt files.
    # Can be changed when invoking the class method
    columns = [
        ("division_id", int, str),
        ("division_cde", str, str),
        ("division_name", str, str),
        ("comments", str, str),
        ]

    # The file delimiter for the text file.
    sep = "\t|\t"
    end = "\t|\n"
    header = False


class GenCode(BaseTable, Base):
    """ Table schema for "gencode.dmp" from taxdmp folder.

    ##Has a weird file format, needs special parser

    Description of file columns:

    genetic code id -- GenBank genetic code id
    abbreviation -- genetic code name abbreviation
    name -- genetic code name
    cde -- translation table for this genetic code
    starts -- start codons for this genetic code

    """

    code_id = Column(String)
    abbreviation = Column(String)
    name = Column(String)
    cde = Column(String)
    starts = Column(String)


def main(db, nodes, names, division, debug=False):
    engine = create_engine(db, echo=False)
    Base.metadata.create_all(engine)
    with session_scope(engine) as session:
        Nodes.from_file(nodes, session=session)
        Names.from_file(names, session=session)
        Division.from_file(division, session=session)
    return

def cli(args=sys.argv):
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
        "-n", "--nodes",
        help=(
            "Path to the nodes database file."
            )
        )
    arg_parser.add_argument(
        "-a", "--names",
        help=(
            "Path to the names database file."
            )
        )
    arg_parser.add_argument(
        "-e", "--division",
        help="Path to the divisions database file."
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
    main(**args.__dict__)


if __name__ == '__main__':
    cli()
