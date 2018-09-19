import pymysql as sql
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

    def create_derived(self, tbl):
        labevents_tbl_str = """mimic.LABEVENTS"""
        labitems_tbl_str = """mimic.D_LABITEMS"""
        derived_tbl_str = f"""derived.{tbl}"""
        create_str = """ROW_ID UNSIGNED INT, SUBJECT_ID UNSIGNED INT, HADM_ID UNSIGNED INT, LOINC_CODE VARCHAR(100), 
        CHARTTIME TIMESTAMP(0), VALUE VARCHAR(200), VALUENUM DOUBLE, VALUEUOM VARCHAR(20), FLAG VARCHAR(20) """
        select_labevents_str = """SELECT ROW_ID, SUBJECT_ID, HADM_ID, CHARTTIME, VALUE, VALUENUM, VALUEUOM, FLAG"""
        select_labitems_str = """SELECT LOINC_CODE"""

        exec_str = f"""CREATE TABLE {derived_tbl_str}{create_str} AS {select_labevents_str} FROM {labevents_tbl_str} UNION {select_labitems_str} FROM {labitems_tbl_str}"""
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
    example.create_derived('loinc_labevents')
