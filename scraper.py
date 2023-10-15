import pandas as pd
from bs4 import BeautifulSoup as bs
import requests
import json
from sqlalchemy import create_engine
from Connection_test import connection_string
def CLimb_scrape():
    try:
        with open('crag_ids.txt', 'r') as crag_file:
            crag_id = crag_file.readline()

        engine = create_engine(connection_string)

        site = f"https://www.ukclimbing.com/logbook/crag.php?id={crag_id}"
        results = requests.get(site)
        soup = bs(results.content, 'html.parser')
        scripts = soup.find_all('script')
        climb_table = scripts[10].string

        start_keyword = 'table_data = ['
        start_index = climb_table.find(start_keyword) + len('table_data = ')
        open_brackets = 0
        close_brackets = 0
        i = start_index

        while True:
            if climb_table[i] == '[':
                open_brackets += 1
            elif climb_table[i] == ']':
                close_brackets += 1
            if open_brackets == close_brackets and open_brackets != 0:
                end_index = i + 1
                break
            i += 1
        print("accessing json")
        json_data = climb_table[start_index:end_index]
        climbs = json.loads(json_data)
        climbs_df = pd.DataFrame(climbs)
        filtered_climbs_df = climbs_df.filter(['id', 'name', 'grade', 'techgrade', 'gradesystem', 'gradetype', 'gradescore'], axis=1)

        grades_start_keyword = 'grade_list = '
        grade_start_index = climb_table.find(grades_start_keyword) + len('grade_list = ')
        open_swirlies = 0
        close_swirlies = 0
        j = grade_start_index
        while True:
            if climb_table[j] == '{':
                open_swirlies += 1
            elif climb_table[j] == '}':
                close_swirlies += 1
            if open_swirlies == close_swirlies and open_swirlies != 0:
                grade_end_index = j + 1
                break
            j += 1
        json_grades = climb_table[grade_start_index:grade_end_index]
        grades = json.loads(json_grades)
        grades_list = []
        for outer_key, outer_value in grades.items():
            for inner_key, inner_value in outer_value.items():
                row = inner_value.copy()
                if 'alt' in row:
                    alt_data = row.pop('alt')
                    for k, v in alt_data.items():
                        row[f'alt_{k}'] = v
                grades_list.append(row)
        print("pushing to df")
        grades_df = pd.DataFrame(grades_list)

        climbs_w_grades = pd.merge(
            filtered_climbs_df, grades_df, left_on='grade', right_on='id')
        climbs_w_grades.drop(['grade', 'gradesystem_x', 'id_y', 'gradesystem_y', 'score', 'gradecolor', 'alt_id', 'alt_name'], axis=1, inplace=True, errors="ignore")
        climbs_w_grades.rename(columns={'id_x': 'climb_id', 'name_x': 'climb_name', 'name_y': 'climb_grade'}, inplace=True)
        climbs_w_grades = climbs_w_grades[['climb_id', 'climb_name', 'climb_grade', 'techgrade', 'gradescore', 'gradetype']]

        climbs_w_grades.to_sql(name="climbing_data", con=engine, if_exists="append", index=False)
        print(f"crag {crag_id} pushed to DB successfuly")

    except Exception as e:
        with open('error_log.txt', 'a') as log_file:
            log_file.write(f"Encounted error - {e} while processing crag  id {crag_id}")
        print(e)

    finally:
        with open("crag_ids.txt", "r+") as crag_file:
            crags = crag_file.readlines()
            crag_file.seek(0)
            crag_file.truncate()
            crag_file.writelines(crags[1:])

CLimb_scrape()