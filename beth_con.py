import re
from db_util import *
from getpass import getpass


class BethCon:
    def __init__(self):
        self.handles = DerivedKnowledgeHandles()

    def parse_beth_con_file(self, tbl):
        file_path = "~/i2b2/concept_assertion_relation_training_data/beth/concept/record-13.con"
        file = open(file_path, "r")
        for line in file:
            condition = re.findall(r'c\="(.*?)"', line)
            types = re.findall(r't\="(.*?)"', line)
            line_num = re.findall(r'" (.*?):', line)
        capstone_tbl_str = f'''capstone.{tbl}'''


if __name__ == '__main__':
    usr = 'greenes2018'
    pw = getpass()
    cap_db = 'capstone'
    cap_host = 'db01.healthcreek.org'

    beth_con = BethCon()
    beth_con.parse_beth_con_file("BETHCON")