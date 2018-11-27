import MySQLdb as sql
import MySQLdb.connections as conns
from getpass import getpass
from dataclasses import dataclass


class DatabaseHandle:
    connection: conns.Connection = None
    cursor: conns.cursors.Cursor = None
    user: str = None
    host: str = None
    db: str = None

    def __init__(self, user, pw, db, host):
        self.connection = sql.connect(user=user, pw=pw, db=db, host=host)
        self.user = user
        self.host = host
        self.db = db


@dataclass
class DerivedKnowledgeHandles:
    mimic: DatabaseHandle
    derived: DatabaseHandle
    capstone: DatabaseHandle


@dataclass
class BethKnowledgeHandle:
    capstone: DatabaseHandle


@dataclass
class TestKnowledgeHandle:
    capstone: DatabaseHandle
