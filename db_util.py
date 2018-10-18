import MySQLdb as sql
from getpass import getpass
from dataclasses import dataclass


class DatabaseHandle:
    connection: sql.connections.Connection = None
    cursor: sql.cursors.Cursor = None
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
