import os
import re
import sys

import pytest

# Make the repo-root modules (tempest_poller, clickhouse_schema, ...) importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class FakeQueryResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClickHouse:
    """
    In-memory stand-in for a clickhouse_connect client. Emulates
    ReplacingMergeTree + FINAL: rows with the same ORDER BY key collapse to the
    last one inserted.
    """

    FROM_RE = re.compile(r"FROM `(\w+)`\.`(\w+)` FINAL")

    def __init__(self, tables):
        self.tables = {t.name: t for t in tables}
        self.data = {}  # (database, table) -> {key: {col: value}}
        self.inserts = []
        self.commands = []

    def ping(self):
        return True

    def command(self, sql):
        self.commands.append(sql)

    def insert(self, table, data, column_names, database):
        t = self.tables[table]
        assert column_names == t.column_names
        store = self.data.setdefault((database, table), {})
        for values in data:
            assert len(values) == len(column_names)
            row = dict(zip(column_names, values))
            store[tuple(row[k] for k in t.key)] = row
        self.inserts.append((database, table, len(data)))

    def query(self, sql, parameters=None):
        database, table = self.FROM_RE.search(sql).groups()
        t = self.tables[table]
        start, stop = parameters["start"], parameters["stop"]
        cols = ["time"] + [c for c in t.column_names if c != "time"]
        rows = [
            [r[c] for c in cols]
            for r in self.data.get((database, table), {}).values()
            if start <= r["time"] < stop
        ]
        return FakeQueryResult(rows)

    def delete_where(self, database, table, pred):
        store = self.data[(database, table)]
        for k in [k for k, r in store.items() if pred(r)]:
            del store[k]

    def close(self):
        pass


@pytest.fixture
def fake_clickhouse():
    import clickhouse_schema as chs

    return FakeClickHouse([t for p in chs.PIPELINES.values() for t in p["tables"]])
