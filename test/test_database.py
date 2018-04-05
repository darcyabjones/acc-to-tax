"""
"""

import os
import re
from unittest.mock import MagicMock

import pytest

from acc2tax.database import int2bool
from acc2tax.database import bool2int
from acc2tax.database import Base
from acc2tax.database import BaseTable
from acc2tax.database import Acc2Tax
from acc2tax.database import Nodes
from acc2tax.database import Names
from acc2tax.database import Division
from acc2tax.database import GenCode

# Define fixtures

@pytest.fixture()
def session():
    """ Sets up an in-memory sqlite database for testing. """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    # Setup
    engine = create_engine("sqlite:///:memory:", echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    Base.metadata.create_all(engine)

    yield session

    # Teardown
    session.close()


@pytest.fixture()
def nodes_table(session):
    Nodes.from_file("test/data/sample_nodes.dmp", session)
    return Nodes

@pytest.mark.parametrize("expected,i", [
    (True, '1'),
    (False, '0'),
    ])
def test_int2bool(i, expected):
    assert int2bool(i) == expected
    return

@pytest.mark.parametrize("i,expected", [
    (True, '1'),
    (False, '0'),
    ])
def test_bool2int(i, expected):
    assert bool2int(i) == expected
    return

class TestBaseTable(object):

    parser_test_lines = [
        {
            "line": "one\t2\t0\n",
            "sep": "\t",
            "end": "\n",
            "columns": [("a", str, str), ("b", int, str), ("c", int2bool, bool2int)],
            "expected": {"a": "one", "b": 2, "c": False},
        },
        {
            "line": "1234\t|\t567\t|\teight\t|\n",
            "sep": "\t|\t",
            "end": "\t|\n",
            "columns": [("a", int, str), ("b", str, str), ("c", str, str)],
            "expected": {"a": 1234, "b": "567", "c": "eight"}
        },
        {
            "line": "1234\t|\t567\t|\t\t|\n",
            "sep": "\t|\t",
            "end": "\t|\n",
            "columns": [("a", int, str), ("b", str, str), ("c", str, str)],
            "expected": {"a": 1234, "b": "567", "c": ""}
        },
        ]

    parser_test_files = [
        {
            "path": "test/data/sample_nodes.dmp",
            "sep": Nodes.sep,
            "end": Nodes.end,
            "columns": Nodes.columns,
            "header": Nodes.header,
        },
        {
            "path": "test/data/sample_names.dmp",
            "sep": Names.sep,
            "end": Names.end,
            "columns": Names.columns,
            "header": Names.header,
        },
        {
            "path": "test/data/sample_division.dmp",
            "sep": Division.sep,
            "end": Division.end,
            "columns": Division.columns,
            "header": Division.header,
        },
        ]

    writer_test_records = [
        {
            "records": [
                MagicMock(
                    taxid=2,
                    parent_taxid=1,
                    rank="species",
                    embl_code='',
                    division_id=0,
                    inherited_div_flag=False,
                    genetic_code_id='TEST',
                    inherited_genetic_code_flag=True,
                    mitochonchondrial_genetic_code_id="TEST",
                    inherited_mitochonchondrial_genetic_code_flag=True,
                    genbank_hidden_flag=False,
                    hidden_subtree_root_flag=False,
                    comments=''
                    ),
                MagicMock(
                    taxid=3,
                    parent_taxid=1,
                    rank="species",
                    embl_code='',
                    division_id=0,
                    inherited_div_flag=False,
                    genetic_code_id='TEST',
                    inherited_genetic_code_flag=True,
                    mitochonchondrial_genetic_code_id="TEST",
                    inherited_mitochonchondrial_genetic_code_flag=True,
                    genbank_hidden_flag=True,
                    hidden_subtree_root_flag=False,
                    comments=''
                    ),
                ],
            "sep": Nodes.sep,
            "end": Nodes.end,
            "columns": Nodes.columns,
            "header": Nodes.header,
            "expected": [
                    [["2", "1", "species", "", "0", "0", "TEST", "1", "TEST","1", "0", "0", ""],
                 ["3", "1", "species", "", "0", "0", "TEST", "1", "TEST","1", "1", "0", ""],]
                ],
        },
        {
            "records": [
                MagicMock(
                    taxid=2,
                    name_="species1",
                    unique_name="",
                    name_class="Scientific name",
                    ),
                MagicMock(
                    taxid=3,
                    name_="species2",
                    unique_name="species2_1",
                    name_class="Common name",
                    ),
                ],
            "sep": Names.sep,
            "end": Names.end,
            "columns": Names.columns,
            "header": Names.header,
            "expected": [
                "\t|\t".join(x) + "\t|\n" for x in
                [["2", "species1", "", "Scientific name"],
                 ["3", "species2", "species2_1", "Common name"],]
                ],
        },
        ]

    @pytest.mark.parametrize("line,sep,end,columns,expected",
        [
            (l["line"], l["sep"], l["end"], l["columns"], l["expected"])
            for l in parser_test_lines
        ]
        )
    def test_line_trans(self, line, sep, end, columns, expected):
        result = BaseTable.line_trans(line, sep, end, columns)
        print("result:", result)
        for k_exp, v_exp in expected.items():
            assert v_exp == result[k_exp]
        return

    @pytest.mark.parametrize("line,sep,end,columns,expected",
        [
            (l["line"], l["sep"], l["end"], l["columns"], l["expected"])
            for l in parser_test_lines
        ]
        )
    def test_string_fmt(self, line, sep, end, columns, expected):
        obj = MagicMock(**expected)
        result = BaseTable.string_fmt(obj, sep, end, columns)
        print("result:", result)
        assert result == line
        return

    @pytest.mark.parametrize("path,sep,end,columns,header",
        [
            (l["path"], l["sep"], l["end"], l["columns"], l["header"])
            for l in parser_test_files
        ]
        )
    def test_from_file(self, path, sep, end, columns, header):

        session = MagicMock()
        session.commit = MagicMock(return_value=None)
        session.bulk_insert_mappings = MagicMock(return_value=None)

        BaseTable.from_file(
            filepath=path,
            session=session,
            sep=sep,
            columns=columns,
            header=header,
            )

        session.bulk_insert_mappings.assert_called()
        session.commit.assert_called_once()
        return

    @pytest.mark.parametrize("records,sep,end,columns,header,expected",
        [
            (l["records"], l["sep"], l["end"], l["columns"], l["header"], l["expected"])
            for l in writer_test_records
        ]
        )
    def test_to_table(self, records, sep, end, columns, header, expected):
        # Since name is a special attribute for mock we need to assign it 
        # After creation time.
        for record in records:
            record.name = record.name_

        results = BaseTable.to_table(records, sep, end, columns, header)

        for result, exp in zip(results, expected):
            assert result == exp
        return


class TestNodes(object):

    def test_from_file(self, session):
        Nodes.from_file("test/data/sample_nodes.dmp", session)

        record = session.query(Nodes).filter(Nodes.taxid == 1224).one()
        assert record.parent_taxid == 2
        assert record.rank == "phylum"
        return

    def test_get_parents(self, session, nodes_table):
        # Nodes table is already populated with fixture

        Nodes.get_parents()
        return
