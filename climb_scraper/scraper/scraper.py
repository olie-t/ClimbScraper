import requests
import json
import bs4 as bs


def get_data(url: str) -> json:
    """Attempts to get data from the provided URL"""

    try:
        response = requests.get(url)
        return response
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
    """Find the relevant data for both the climbs and the grading at this crag"""

    start_keyword = 'table_data = ['
    try:
        start_index = data.find(start_keyword) + len(start_keyword)
    except Exception as e:
        print(f'Unable to find start keyword in data string:\n{e}')
    open_brackets = 0
    closed_brackets = 0
    i = start_index

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
