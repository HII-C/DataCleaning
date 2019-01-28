import MySQLdb as sql
from getpass import getpass


class DBConnection:
    def __init__(self):
        self.mimic_conn = None
        self.mimic_cur = None
        self.der_conn = None
        self.der_cur = None
        self.semmed_conn = None
        self.semmed_cur = None
        self.mimic_db = {'user': user, 'db': 'mimic', 'host': 'db01.healthcreek.org', 'password': pw}
        self.der_db = {'user': user, 'db': 'derived', 'host': 'db01.healthcreek.org', 'password': pw}
        self.semmed_db = {'user': user, 'db': 'semmed', 'host': 'db01.healthcreek.org', 'password': pw}

    def connect_mimic_db(self, database):
        self.mimic_conn = sql.connect(**database)
        self.mimic_cur = self.mimic_conn.cursor()

    def connect_der_db(self, database):
        self.der_conn = sql.connect(**database)
        self.der_cur = self.der_conn.cursor()

    def connect_semmed_db(self, database):
        self.der_conn = sql.connect(**database)
        self.der_cur = self.der_conn.cursor()


if __name__ == "__main__":
    user = 'root'
    pw = getpass(f'What is the password for the user {user}?\n')
