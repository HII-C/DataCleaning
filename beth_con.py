import re
from db_util import *
import os.path
from getpass import getpass


class TestCon:
    def __init__(self):
        self.handles = TestKnowledgeHandle()

    def test_rel(self, tbl):
        # create the table
        derived_tbl_str = f'derived.{tbl}'
        create_str = '''FILE_NUM UNSIGNED INT, C1 VARCHAR(200), C2 VARCHAR(200), C1_LINE_START UNSIGNED INT,
        C1_LINE_START_OFFSET UNSIGNED INT, C1_LINE_END UNSIGNED INT, C1_LINE_END_OFFSET UNSIGNED INT,
        C2_LINE_START UNSIGNED INT, C2_LINE_START_OFFSET UNSIGNED INT, C2_LINE_END UNSIGNED INT,
        C2_LINE_END_OFFSET UNSIGNED INT, RELATION VARCHAR(10)'''
        exec_str = f'''CREATE TABLE {derived_tbl_str}{create_str}'''
        self.handles.capstone.cursor.execute(exec_str)
        self.handles.capstone.connection.commit()

        # Parse through each relation file and extract the concepts, relations, and line numbers
        for file_num in range(13, 179):
            file_path = "Capstone/i2b2/concept_assertion_relation_training_data/beth/rel/record-" + str(
                file_num) + ".rel"
            if not os.path.isfile(file_path):
                file_num += 1
                continue
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

            c1_line_num_start = []
            c1_line_num_end = []
            c1_offset_start = []
            c1_offset_end = []
            c2_line_num_start = []
            c2_line_num_end = []
            c2_offset_start = []
            c2_offset_end = []

            i = 0
            while i < len(c1_line_num_all):
                if i % 2 == 0:
                    c1_line_num_start.append(c1_line_num_all[i])
                    c1_offset_start.append(c1_offset_all[i])
                    c2_line_num_start.append(c2_line_num_all[i])
                    c2_offset_start.append(c2_offset_all[i])
                elif i % 2 == 1:
                    c1_line_num_end.append(c1_line_num_all[i])
                    c1_offset_end.append(c1_offset_all[i])
                    c2_line_num_end.append(c2_line_num_all[i])
                    c2_offset_end.append(c2_offset_all[i])
                i += 1

            row = 0
            while row < len(c1_line_all):
                curr_c1 = c1[row]
                curr_c1_line_num_start = c1_line_num_start[row]
                curr_c1_line_num_end = c1_line_num_end[row]
                curr_c1_offset_start = c1_offset_start[row]
                curr_c1_offset_end = c1_offset_end[row]
                curr_c2 = c2[row]
                curr_c2_line_num_start = c2_line_num_start[row]
                curr_c2_line_num_end = c2_line_num_end[row]
                curr_c2_offset_start = c2_offset_start[row]
                curr_c2_offset_end = c2_offset_end[row]
                # will use file_num as record number
                exec_str = f'''INSERT INTO {tbl} VALUES ({file_num}, {curr_c1}, {curr_c2}, {curr_c1_line_num_start},
                {curr_c1_offset_start}, {curr_c1_line_num_end}, {curr_c1_offset_end}, {curr_c2_line_num_start}, 
                {curr_c2_offset_start}, {curr_c2_line_num_end}, {curr_c2_offset_end})'''
                self.handles.capstone.cursor.execute(exec_str)
                self.handles.capstone.connection.commit()


if __name__ == '__main__':
    usr = 'greenes2018'
    pw = getpass()
    cap_db = 'capstone'
    cap_host = 'db01.healthcreek.org'

    test_con = TestCon()
    test_con.handles.capstone = DatabaseHandle(usr, pw, cap_db, cap_host)
    test_con.test_rel("BETHCON")