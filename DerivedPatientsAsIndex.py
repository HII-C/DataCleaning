import MySQLdb as sql
from getpass import getpass


class DerivedPatientsAsIndex:
    def __init__(self):
        self.mimic_conn = None
        self.mimic_cur = None
        self.der_conn = None
        self.der_cur = None

    def connect_mimic_db(self, database):
        self.mimic_conn = sql.connect(**database)
        self.mimic_cur = self.mimic_conn.cursor()

    def connect_der_db(self, database):
        self.der_conn = sql.connect(**database)
        self.der_cur = self.der_conn.cursor()

    def create_derived(self, source, tbl):
        mimic_source_str = f"""mimic.{source}"""
        derived_tbl_str = f"""derived.{tbl}"""
        create_str = """ROW_ID UNSIGNED INT, SUBJECT_ID UNSIGNED INT"""
        select_str = """SELECT ROW_ID, SUBJECT_ID"""

        exec_str = f"""CREATE TABLE {derived_tbl_str}{create_str} AS {select_str} FROM {mimic_source_str}"""

        self.mimic_cur.execute(exec_str)
        self.mimic_conn.commit()


if __name__ == "__main__":
    user = 'root'
    pw = getpass(f'What is the password for the user {user}?\n')

    mimic_db = {'user': user, 'db': 'mimic', 'host': 'db01.healthcreek.org', 'password': pw}
    der_db = {'user': user, 'db': 'derived', 'host': 'db01.healthcreek.org', 'password': pw}

    example = DerivedPatientsAsIndex()
    example.connect_mimic_db(mimic_db)
    example.connect_der_db(der_db)
    example.create_derived("PRESCRIPTIONS", "patients_as_index")
