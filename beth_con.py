import re
from db_util import *
import pandas as pd
from getpass import getpass


# Test class to be called and run locally
class TestCon:
    def __init__(self):
        self.handles = TestKnowledgeHandle()

    def test_rel(self, tbl):
        # Parse through each line in line_num, map concept to relation file and get the relation
        file_path = "C:\\i2b2\\concept_assertion_relation_training_data\\beth\\rel\\record-13.rel"
        file = open(file_path, "r")
        concepts = []
        rel = []
        c1_line_all = []
        c2_line_all = []
        for line in file:
            concept = re.findall(r'c\="(.*?)"', line)
            relation = re.findall(r'r\="(.*?)"', line)
            line_num = re.findall(r"\d*:\d*", line)
            concepts += concept
            rel += relation
            c1_line_all += line_num[:2]
            c2_line_all += line_num[2:]

        i = 0
        c1 = []
        c2 = []
        while i < len(concepts):
            if i % 2 == 0:
                c1.append(concepts[i])
                i += 1
            elif i % 2 == 1:
                c2.append(concepts[i])
                i += 1

        i = 0
        c1_line_num_all = []
        c2_line_num_all = []
        c1_offset_all = []
        c2_offset_all = []
        while i < len(c1_line_all):
            c1_line_num_all.append(re.findall(r"(\d*?):", c1_line_all[i]))
            c2_line_num_all.append(re.findall(r"(\d*?):", c2_line_all[i]))
            c1_offset_all.append(re.findall(r":(\d*)", c1_line_all[i]))
            c2_offset_all.append(re.findall(r":(\d*)", c2_line_all[i]))
            i += 1

        # store the data
        derived_tbl_str = f'derived.{tbl}'
        create_str = '''C1 VARCHAR(200), C2 VARCHAR(200), C1_LINE_START UNSIGNED INT, C1_LINE_START_OFFSET UNSIGNED INT,
        C1_LINE_END UNSIGNED INT, C1_LINE_END_OFFSET UNSIGNED INT, C2_LINE_START UNSIGNED INT, C2_LINE_START_OFFSET UNSIGNED INT,
        C2_LINE_END UNSIGNED INT, C2_LINE_END_OFFSET UNSIGNED INT, RELATION VARCHAR(10)'''
        exec_str = f'''CREATE TABLE {derived_tbl_str}{create_str}'''
        self.handles.capstone.cursor.execute(exec_str)
        self.handles.capstone.connection.commit()

# Actual class to be called and run on server
class BethCon:
    def __init__(self):
        self.handles = BethKnowledgeHandle()
        all_conditions = []
        all_types = []
        all_lines = []
        # Parse through each con file and then use regex
        file_path = "Capstone/i2b2/concept_assertion_relation_training_data/beth/concept/record-13.con"
        file = open(file_path, "r")
        for line in file:
            condition = re.findall(r'c\="(.*?)"', line)
            type = re.findall(r't\="(.*?)"', line)
            line_num = re.findall(r'" (.*?):', line)
            all_conditions += condition
            all_types += type
            all_lines += line_num
        # Parse through each line in line_num, parse through txt file at each line num, derive relation
        file_path = "Capstone/i2b2/concept_assertion_relation_training_data/beth/txt/record-13.txt"
        file = open(file_path, "r")
        txt_lines = []
        for line in file:
            print(line)
            txt_lines.append(line.strip())

        print(txt_lines)
        for line in all_lines:
            curr_line = txt_lines[int(line)]

    def parse_beth_con_file(self, tbl):
        all_conditions = []
        all_types = []
        all_lines = []
        # Parse through each con file and then use regex
        file_path = "Capstone/i2b2/concept_assertion_relation_training_data/beth/concept/record-13.con"
        file = open(file_path, "r")
        for line in file:
            condition = re.findall(r'c\="(.*?)"', line)
            type = re.findall(r't\="(.*?)"', line)
            line_num = re.findall(r'" (.*?):', line)
            all_conditions += condition
            all_types += type
            all_lines += line_num
        # Parse through each line in line_num, parse through txt file at each line num, derive relation
        file_path = "Capstone/i2b2/concept_assertion_relation_training_data/beth/txt/record-13.txt"
        file = open(file_path, "r")
        txt_lines = []
        for line in file:
            print(line)
            txt_lines.append(line.strip())

        print(txt_lines)
        for line in all_lines:
            curr_line = txt_lines[int(line)]


if __name__ == '__main__':
    usr = 'greenes2018'
    pw = getpass()
    cap_db = 'capstone'
    cap_host = 'db01.healthcreek.org'

    test_con = TestCon()
    test_con.handles.capstone = DatabaseHandle(usr, pw, cap_db, cap_host)
    test_con.test_rel("BETHCON")
