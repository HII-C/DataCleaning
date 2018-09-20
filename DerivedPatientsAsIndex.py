from DBConnection import DBConnection


class DerivedPatientsAsIndex:
    def __init__(self):
        self.connection = DBConnection()

    def create_derived(self, source, tbl):
        mimic_source_str = f"""mimic.{source}"""
        derived_tbl_str = f"""derived.{tbl}"""
        create_str = """ROW_ID UNSIGNED INT, SUBJECT_ID UNSIGNED INT"""
        select_str = """SELECT ROW_ID, SUBJECT_ID"""

        exec_str = f"""CREATE TABLE {derived_tbl_str}{create_str} AS {select_str} FROM {mimic_source_str}"""
        self.connection.mimic_cur.execute(exec_str)
        self.connection.mimic_conn.commit()


if __name__ == "__main__":
    example = DerivedPatientsAsIndex()
    example.connection.connect_mimic_db(example.connection.mimic_db)
    example.connection.connect_der_db(example.connection.der_db)
    example.create_derived("PRESCRIPTIONS", "patients_as_index")
