from typing import Optional

import requests
import json
from bs4 import BeautifulSoup as bs
import pandas as pd
from pandas.core.interchange import dataframe
import random
import time
from scraper.list_builder import is_valid_crag_page

def get_random_user_agent() -> str:
    """Return a random user agent string"""
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15'
    ]
    return random.choice(user_agents)

def get_crag_id(txt_file: str) -> int:
    """ Gets first crag id from text file"""
    try:
        with open(txt_file, 'r') as fin:
            data = fin.read().splitlines(True)
            if not data:  # Check if file is empty
                return None
            crag_id = data[0].strip()
        with open(txt_file, 'w') as fout:
            fout.writelines(data[1:])
        return int(crag_id)
    except Exception as e:
        print(f"Failed to get crag id from {txt_file} with error: \n{e}")
        return None

def url_builder(crag_id:int) -> str:
    """Generates a url from a crag ID"""
    if crag_id is None:
        return None
    return f"https://www.ukclimbing.com/logbook/crag.php?id={crag_id}"


def get_data(url: str) -> Optional[requests.Response]:
    """Make requests appear more human-like"""
    headers = {
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }

    try:
        time.sleep(random.uniform(3, 7))
        session = requests.Session()
        response = session.get(url, headers=headers)

        # Check for various status codes
        if response.status_code == 410:  # Gone
            print(f"Page gone (410) for URL: {url}")
            return None
        elif response.status_code == 404:  # Not Found
            print(f"Page not found (404) for URL: {url}")
            return None
        elif response.status_code != 200:  # Any other non-200 status
            print(f"Unexpected status code {response.status_code} for URL: {url}")
            return None

        # Check if it's a valid crag page
        if is_valid_crag_page(response.content):
            return response
        else:
            print(f"Not a valid crag page: {url}")
            return None

    except Exception as e:
        print(f"Get Data request failed with error:\n{e}")
        return None


def data_to_string(data: json) -> str:
    """Processes the raw response into a string for processing"""
    if data is None:
        return None

    try:
        soup = bs(data.content, 'html.parser')
        scripts = soup.find_all('script')

        # Look specifically for the script containing table_data
        table_data = None
        for script in scripts:
            if script.string and 'table_data = ' in script.string:
                # Get the whole script content
                table_data = script.string.strip()
                break

        if table_data is None:
            print("Could not find table_data in scripts")
            return None

        return table_data
    except Exception as e:
        print(f"Data processor request failed with error:\n{e}")
        return None

def string_processor_climbs(data: str) -> json:
    """Find the relevant data for the climbs at this crag"""
    if data is None:
        return None

    try:
        start_keyword = 'table_data = '
        start_index = data.find(start_keyword)
        if start_index == -1:
            print('Unable to find climb data in page')
            return None

        start_index += len(start_keyword)

        # Find the semicolon that terminates the JavaScript statement
        end_index = data.find('\n', start_index)
        if end_index == -1:
            end_index = data.find(';', start_index)
        if end_index == -1:
            print('Unable to find end of table_data')
            return None

        # Extract the raw JSON string
        json_str = data[start_index:end_index].strip()

        # Remove any trailing commas that might cause JSON parsing errors
        json_str = json_str.rstrip(',')

        # Remove any trailing semicolons
        json_str = json_str.rstrip(';')

        # Test parse the JSON
        try:
            json.loads(json_str)
            return json_str
        except json.JSONDecodeError as e:
            print(f"Invalid JSON format: {e}")
            print("First 100 characters of extracted data:", json_str[:100])
            # Debug output to help identify issues
            print("Last 50 characters:", json_str[-50:])
            return None

    except Exception as e:
        print(f'String processing failed with error:\n{e}')
        print("Data string preview:", data[:200] if data else "None")
        return None


def climbs_dataframe_creation(data: json) -> dataframe:
    """Takes the climb data in json and returns it in a dataframe with only the relevant information"""
    if data is None:
        return None

    try:
        # Parse the JSON data
        climbs_data = json.loads(data)

        # Ensure we have a list of climb data
        if not isinstance(climbs_data, list):
            print("Climbs data is not in expected list format")
            return None

        if not climbs_data:  # Check if the data is empty
            print("No climbs data found in JSON")
            return None

        # Create dataframe
        climbs_dataframe = pd.DataFrame(climbs_data)

        # Check for required columns
        required_columns = ['id', 'name', 'grade', 'techgrade', 'gradesystem', 'gradetype', 'gradescore']
        missing_columns = [col for col in required_columns if col not in climbs_dataframe.columns]
        if missing_columns:
            print(f"Missing required columns: {missing_columns}")
            return None

        # Filter columns
        climbs_dataframe_filtered = climbs_dataframe[required_columns]
        return climbs_dataframe_filtered

    except json.JSONDecodeError as e:
        print(f'JSON parsing error: {e}')
        print("Problematic JSON:", data[:200])
        return None
    except Exception as e:
        print(f'Failed to create climbs dataframe with error:\n{e}')
        print("Data type:", type(data))
        return None

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

