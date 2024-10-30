import requests
import json
import bs4 as bs
import pandas as pd
from pandas.core.interchange import dataframe

from scraper.list_builder import is_valid_crag_page


def get_crag_id(txt_file: str) -> int:
    """ Gets first crag id from text file"""
    try:
        with open(txt_file, 'r') as fin:
            data = fin.read().splitlines(True)
            crag_id = data[0]
        with open(txt_file, 'w') as fout:
            fout.writelines(data[1:])
        return crag_id
    except Exception as e:
        print(f"Failed to get crag id from {txt_file} with error: \n{e}")

def url_builder(crag_id:int) -> str:
    """Generates a url from a crag ID"""
    return f"https://www.ukclimbing.com/logbook/crag.php?id={crag_id}"


def get_data(url: str) -> json:
    """Attempts to get data from the provided URL"""

    try:
        response = requests.get(url)
        if response.status_code == 200 and is_valid_crag_page(response.content):
            return response
        else:
            return None
    except Exception as e:
        print(f"Get Data request failed with error:\n{e}")


def data_to_string(data: json) -> str:
    """Processes the raw response into a string for processing"""

    try:
        soup = bs(data.content, 'html.parser')
        scripts = soup.find_all('script')
        climb_table_string = scripts[10].string
        return climb_table_string
    except Exception as e:
        print(f"Data processor request failed with error:\n{e}")


def string_processor_climbs(data: str) -> json:
    """Find the relevant data for the climbs at this crag"""

    start_keyword = 'table_data = ['
    try:
        start_index = data.find(start_keyword) + len(start_keyword)
        i = start_index
    except Exception as e:
        print(f'Unable to find start keyword in data string:\n{e}')
    open_brackets = 0
    closed_brackets = 0

    try:
        while True:
            if data[i] == '[':
                open_brackets += 1
            elif data[i] == ']':
                closed_brackets += 1
            if open_brackets == closed_brackets and open_brackets != 0:
                end_index = i + 1
                break
            i += 1
        json_data = data[start_index:end_index]
        return json_data
    except Exception as e:
        print(f'String processing failed to correctly determine the bounds of the json with error:\n{e}')

def climbs_dataframe_creation(data: json) -> dataframe:
    """Takes the climb data in json and returns it in a dataframe with only the relveant information"""
    climbs_data = json.loads(data)
    climbs_dataframe = pd.DataFrame(climbs_data)
    climbs_dataframe_filtered = climbs_dataframe.filter(['id', 'name', 'grade', 'techgrade', 'gradesystem',
                                                         'gradetype', 'gradescore'], axis=1)
    return climbs_dataframe_filtered

def string_processor_grades(data: str) -> json:
    """Find the relevant data for the grades at this crag"""

    start_keyword = 'grades_list = '
    try:
        start_index = data.find(start_keyword) + len(start_keyword)
        i = start_index
    except Exception as e:
        print(f'Unable to find start keyword in data string:\n{e}')
    open_brackets = 0
    closed_brackets = 0


    try:
        while True:
            if data[i] == '{':
                open_brackets += 1
            elif data[i] == '}':
                closed_brackets += 1
            if open_brackets == closed_brackets and open_brackets != 0:
                end_index = i + 1
                break
            i += 1
        json_data = data[start_index:end_index]
        return json_data
    except Exception as e:
        print(f'String processing failed to correctly determine the bounds of the json with error:\n{e}')

def grades_dataframe_creation(data: json) -> dataframe:
    """Takes the grade data in json and returns it in dataframe with only the relevant information"""
    try:
        grades_json = json.loads(data)
        grades_list = []
        for outer_value in grades_json.values():
            for inner_value in outer_value.values():
                row = inner_value.copy()
                if 'alt' in row:
                    alt_data = row.pop('alt')
                    row.update({f'alt_{k}': v for k, v in alt_data.items()})
                grades_list.append(row)
        grades_df = pd.DataFrame(grades_list)
        return grades_df
    except Exception as e:
        print(f'Unable to process grades data into dataframe with error:\n{e}')

def climbs_grades_dataframe_merge(climbs: dataframe, grades: dataframe) -> dataframe:
    """Combines the 2 dataframes in preperation for insertion to the database"""

    try:
        climbs_w_grades = pd.merge(climbs, grades, left_on='grade', right_on='id')
        climbs_w_grades.drop(
        ['grade', 'gradesystem_x', 'id_y', 'gradesystem_y', 'score', 'gradecolor', 'alt_id', 'alt_name'], axis=1,
            inplace=True, errors="ignore")
        climbs_w_grades.rename(columns={'id_x': 'climb_id', 'name_x': 'climb_name', 'name_y': 'climb_grade'},
                               inplace=True)
        climbs_w_grades = climbs_w_grades[['climb_id', 'climb_name', 'climb_grade', 'techgrade', 'gradescore',
                                           'gradetype']]
        return climbs_w_grades
    except Exception as e:
        print(f'Unable to merge dataframes with error:\n{e}')

