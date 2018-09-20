from DBConnection import DBConnection


class DerivedPatientsAsIndex:
    def __init__(self):
        self.connection = DBConnection()

    def create_derived(self, tbl):
        labevents_tbl_str = """mimic.LABEVENTS"""
        labitems_tbl_str = """mimic.D_LABITEMS"""
        derived_tbl_str = f"""derived.{tbl}"""
        create_str = """ROW_ID UNSIGNED INT, SUBJECT_ID UNSIGNED INT, HADM_ID UNSIGNED INT, LOINC_CODE VARCHAR(7), 
        CHARTTIME TIMESTAMP(0), VALUE VARCHAR(200), VALUENUM DOUBLE, VALUEUOM VARCHAR(20), FLAG VARCHAR(20) """
        select_labevents_str = """SELECT ROW_ID, SUBJECT_ID, HADM_ID, CHARTTIME, VALUE, VALUENUM, VALUEUOM, FLAG"""
        select_labitems_str = """SELECT LOINC_CODE"""

        exec_str = f"""CREATE TABLE {derived_tbl_str}{create_str} AS {select_labevents_str} FROM {labevents_tbl_str} UNION {select_labitems_str} FROM {labitems_tbl_str}"""
        self.connection.mimic_cur.execute(exec_str)
        self.connection.mimic_conn.commit()


if __name__ == "__main__":
    example = DerivedPatientsAsIndex()
    example.connection.connect_mimic_db(example.connection.mimic_db)
    example.connection.connect_der_db(example.connection.der_db)
    example.create_derived('loinc_labevents')
