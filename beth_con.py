import re
from db_util import *
from getpass import getpass


class BethCon:
    all_conditions = []
    all_types = []
    all_lines = []
    # Parse through each con file and then use regex
    file_path = "C:\\i2b2\\concept_assertion_relation_training_data\\beth\\concept\\record-13.con"
    file = open(file_path, "r")
    for line in file:
        condition = re.findall(r'c\="(.*?)"', line)
        type = re.findall(r't\="(.*?)"', line)
        line_num = re.findall(r'" (.*?):', line)
        all_conditions += condition
        all_types += type
        all_lines += line_num
    # Parse through each line in line_num, parse through txt file at each line num, derive relation
    file_path = "C:\\i2b2\\concept_assertion_relation_training_data\\beth\\txt\\record-13.txt"
    file = open(file_path, "r")
    txt_lines = []
    for line in file:
        print(line)
        txt_lines.append(line.strip())

    print(txt_lines)
    for line in all_lines:
        curr_line = txt_lines[int(line)]



    def parse_beth_con_file(self, tbl):
        global condition
        global types
        global line_num
        # Parse through each con file and then use regex
        file_path = "~/i2b2/concept_assertion_relation_training_data/beth/concept/record-13.con"
        file = open(file_path, "r")
        for line in file:
            condition = re.findall(r'c\="(.*?)"', line)
            types = re.findall(r't\="(.*?)"', line)
            line_num = re.findall(r'" (.*?):', line)
        # Parse through each line in line_num, parse through txt file at each line num, derive relation
        file_path = "~/i2b2/concept_assertion_relation_training_data/beth/txt/record-13.txt"
        file = open(file_path, "r")
        print(line_num)
        self.handles.capstone.cursor.execute("")
        self.handles.capstone.connection.commit()


if __name__ == '__main__':
    usr = 'greenes2018'
    pw = getpass()
    cap_db = 'capstone'
    cap_host = 'db01.healthcreek.org'

    beth_con = BethCon()
    beth_con.handles.capstone = DatabaseHandle(usr, pw, cap_db, cap_host)
    beth_con.parse_beth_con_file("BETHCON")