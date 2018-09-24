from DBConnection import DBConnection


class DerivedTableGeneration:
    def __init__(self):
        self.connection = DBConnection()

    def create_derived_patients_as_index(self, source, tbl):
        mimic_source_str = f"""mimic.{source}"""
        derived_tbl_str = f"""derived.{tbl}"""
        create_str = """ROW_ID UNSIGNED INT, SUBJECT_ID UNSIGNED INT"""
        select_str = """SELECT ROW_ID, SUBJECT_ID"""

        exec_str = f"""CREATE TABLE {derived_tbl_str}{create_str} AS {select_str} FROM {mimic_source_str}"""
        self.connection.mimic_cur.execute(exec_str)
        self.connection.mimic_conn.commit()

    def create_derived_visits_as_index(self, source, tbl):
        mimic_source_str = f"""mimic.{source}"""
        derived_tbl_str = f"""derived.{tbl}"""
        create_str = """ROW_ID UNSIGNED INT, HADM_ID UNSIGNED INT"""
        select_str = """SELECT ROW_ID, HADM_ID"""

        exec_str = f"""CREATE TABLE {derived_tbl_str}{create_str} AS {select_str} FROM {mimic_source_str}"""
        self.connection.mimic_cur.execute(exec_str)
        self.connection.mimic_conn.commit()

    def create_derived_loinc_labevents(self, tbl):
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

    def create_derived_patient_has_diabetes(self, source, tbl):
        mimic_source_str = f"""mimic.{source}"""
        derived_tbl_str = f"""derived.{tbl}"""
        create_str = """SUBJECT_ID UNSIGNED INT, HAS_TARGET TINYINT UNSIGNED"""
        select_str = """SELECT SUBJECT_ID, HAS_TARGET"""
        # Todo: figure out where to retrieve the value for HAS_TARGET

        exec_str = f"""CREATE TABLE {derived_tbl_str}{create_str} AS {select_str} FROM {mimic_source_str}"""
        self.connection.mimic_cur.execute(exec_str)
        self.connection.mimic_conn.commit()


if __name__ == "__main__":
    d_tbl_gen = DerivedTableGeneration()
    d_tbl_gen.connection.connect_mimic_db(d_tbl_gen.connection.mimic_db)
    d_tbl_gen.connection.connect_der_db(d_tbl_gen.connection.der_db)

    d_tbl_gen.create_derived_patients_as_index("PRESCRIPTIONS", "patients_as_index")
    d_tbl_gen.create_derived_visits_as_index("PRESCRIPTIONS", "visits_as_index")
    d_tbl_gen.create_derived_loinc_labevents('loinc_labevents')
    # d_tbl_gen.create_derived_patient_has_diabetes("PRESCRIPTIONS", "patient_has_diabetes")
