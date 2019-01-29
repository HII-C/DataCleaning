from db_util import *
from getpass import getpass


class DerivedTableGeneration:
    def __init__(self):
        self.handles = DerivedKnowledgeHandles()

    def create_derived_patients_as_index(self, source, tbl):
        mimic_source_str = f'mimic.{source}'
        derived_tbl_str = f'derived.{tbl}'
        create_str = '''(ROW_ID UNSIGNED INT AUTO_INCREMENT NOT NULL,
                        SUBJECT_ID MEDIUMINT UNSIGNED,
                        PRIMARY KEY (ROW_ID))'''
        select_str = 'SELECT (ROW_ID, SUBJECT_ID)'

        exec_str = f'''CREATE TABLE
                        {derived_tbl_str} {create_str}
                    AS
                        {select_str}
                    FROM
                        {mimic_source_str}'''
        self.handles.mimic.cursor.execute(exec_str)
        self.handles.mimic.connection.commit()

    def create_derived_visits_as_index(self, source, tbl):
        mimic_source_str = f'mimic.{source}'
        derived_tbl_str = f'derived.{tbl}'
        create_str = '(ROW_ID INT UNSIGNED, HADM_ID INT UNSIGNED, PRIMARY KEY (ROW_ID))'
        select_str = '(SELECT ROW_ID, HADM_ID)'

        exec_str = f'''CREATE TABLE
                            {derived_tbl_str} {create_str}
                        AS
                            {select_str}
                        FROM
                            {mimic_source_str}'''
        self.handles.mimic.cursor.execute(exec_str)
        self.handles.mimic.connection.commit()

    def create_derived_loinc_labevents(self, tbl):
        labevents_tbl_str = 'mimic.LABEVENTS'
        labitems_tbl_str = 'mimic.D_LABITEMS'
        derived_tbl_str = f'derived.{tbl}'
        create_str = '''ROW_ID UNSIGNED INT, SUBJECT_ID UNSIGNED INT, HADM_ID UNSIGNED INT, LOINC_CODE VARCHAR(7), 
        CHARTTIME TIMESTAMP(0), VALUE VARCHAR(200), VALUENUM DOUBLE, VALUEUOM VARCHAR(20), FLAG VARCHAR(20)'''
        select_labevents_str = '''SELECT ROW_ID, SUBJECT_ID, HADM_ID, CHARTTIME, VALUE, VALUENUM, VALUEUOM, FLAG'''
        select_labitems_str = '''SELECT LOINC_CODE'''

        exec_str = f'''CREATE TABLE {derived_tbl_str}{create_str} AS {select_labevents_str} FROM {labevents_tbl_str} 
        UNION {select_labitems_str} FROM {labitems_tbl_str}'''
        self.handles.mimic.cursor.execute(exec_str)
        self.handles.mimic.connection.commit()

    def create_derived_patient_has_diabetes(self, source, tbl):
        mimic_source_str = f'''mimic.{source}'''
        derived_tbl_str = f'''derived.{tbl}'''
        create_str = '''SUBJECT_ID UNSIGNED INT, HAS_TARGET TINYINT UNSIGNED'''
        select_str = '''SELECT SUBJECT_ID, HAS_TARGET'''
        # Todo: figure out where to retrieve the value for HAS_TARGET

        exec_str = f'''CREATE TABLE {derived_tbl_str}{create_str} AS {select_str} FROM {mimic_source_str}'''
        self.handles.mimic.cursor.execute(exec_str)
        self.handles.mimic.connection.commit()

    def create_derived_loinc_labevents_min(self, tbl):
        labevents_tbl_str = '''mimic.LABEVENTS'''
        labitems_tbl_str = '''mimic.D_LABITEMS'''
        derived_tbl_str = f'''derived.{tbl}'''
        create_str = '''ROW_ID UNSIGNED INT, SUBJECT_ID UNSIGNED INT, HADM_ID UNSIGNED INT, LOINC_CODE VARCHAR(7),
        FLAG VARCHAR(20)'''
        select_labevents_str = '''SELECT ROW_ID, SUBJECT_ID, HADM_ID, FLAG'''
        select_labitems_str = '''SELECT LOINC_CODE'''

        exec_str = f'''CREATE TABLE {derived_tbl_str} {create_str} AS {select_labevents_str} from {labevents_tbl_str}
        UNION {select_labitems_str} FROM {labitems_tbl_str}'''
        self.handles.mimic.cursor.execute(exec_str)
        self.handles.mimic.connection.commit()

    def create_semmed_derivation(self, source, tbl):
        semmed_source_str = f'''semmed.{source}'''
        derived_tbl_str = f"""derived.{tbl}"""
        create_str = '''PREDICATE VARCHAR(50), SUBJECT_CUI VARCHAR(255), OBJECT_CUI VARCHAR(255), COUNT INT NOT NULL DEFAULT 1'''
        # number of rows in predication table is 93974376
        # Check with Austin, but instead of trying to query for number of rows in predication table in order to sample,
        # we should use this number instead to help with performance

        # get random sample of 100 subject_cui, then find all occurrences of each subject_cui in predications table
        select_str = '''SELECT SUBJECT_CUI'''
        exec_str = f'''{select_str} FROM {semmed_source_str} ORDER BY RAND() LIMIT 100'''
        self.handles.semmed.cursor.execute(exec_str)
        sample_subject_cui = list(self.handles.derived.cursor.fetchall())
        exec_str = f'''{select_str} FROM {semmed_source_str}'''
        self.handles.semmed.cursor.execute(exec_str)
        sem_subject_cui = list(self.handles.semmed.cursor.fetchall())
        subject_match = [0] * len(sem_subject_cui)
        # finds the indices in predications table that match the subject_cui in the semmed_derivation table
        for i in range(len(sem_subject_cui)):
            for k in range(len(sample_subject_cui)):
                if sample_subject_cui[k] == sem_subject_cui[i]:
                    subject_match[i] = 1
        # create a new table with distinct matches, then compare the two tables to find duplicates
        exec_str = f'''CREATE TABLE {derived_tbl_str}{create_str}'''
        self.handles.derived.cursor.execute(exec_str)
        self.handles.derived.connection.commit()
        for i in range(len(subject_match)):
            if subject_match[i] == 1:
                select_str = '''SELECT PREDICATE, SUBJECT_CUI, OBJECT_CUI'''
                exec_str = f'''INSERT {derived_tbl_str} {select_str} from {semmed_source_str} LIMIT 1 OFFSET {i}'''
                self.handles.semmed.cursor.execute(exec_str)
                self.handles.semmed.connection.commit()


if __name__ == '__main__':
    usr = 'greenes2018'
    pw = getpass()
    mimic_db = 'mimic'
    mimic_host = 'db01.healthcreek.org'
    der_db = 'derived'
    der_host = 'db01.healthcreek.org'
    semmed_db = 'semmed'
    semmed_host = 'db01.healthcreek.org'

    d_tbl_gen = DerivedTableGeneration()
    d_tbl_gen.handles.mimic = DatabaseHandle(usr, pw, mimic_db, mimic_host)
    d_tbl_gen.handles.derived = DatabaseHandle(usr, pw, der_db, der_host)
    d_tbl_gen.handles.semmed = DatabaseHandle(usr, pw, semmed_db, semmed_host)

    d_tbl_gen.create_derived_patients_as_index(
        'PRESCRIPTIONS', 'patients_as_index')
    d_tbl_gen.create_derived_visits_as_index(
        'PRESCRIPTIONS', 'visits_as_index')
    d_tbl_gen.create_derived_loinc_labevents('loinc_labevents')
    # d_tbl_gen.create_derived_patient_has_diabetes('PRESCRIPTIONS', 'patient_has_diabetes')
    d_tbl_gen.create_derived_loinc_labevents_min('loinc_labevents_min')

    d_tbl_gen.create_semmed_derivation('PREDICATION', 'semmed_derivation')
