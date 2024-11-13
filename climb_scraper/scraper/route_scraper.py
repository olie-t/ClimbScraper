import random

from climb_scraper.scraper.scraper_functions import get_crag_id, url_builder, get_data, data_to_string, \
    string_processor_climbs, climbs_dataframe_creation, string_processor_grades, grades_dataframe_creation, \
    climbs_grades_dataframe_merge
from scraper.list_builder import populate_ids_list
import time

def main():
    while True:
        # Check remaining crag IDs
        with open('crag_ids.txt', 'r') as f:
            lines = f.read().splitlines()
            print(f"Length of files: {len(lines)}")

        if len(lines) < 5:
            print("Not enough crag IDs remaining, populating")
            populate_ids_list()

        # Get and process crag data
        crag_id = get_crag_id("crag_ids.txt")
        if crag_id is None:
            print("Failed to get valid crag ID, skipping...")
            continue

        print(f"Processing crag ID: {crag_id}")

        crag_url = url_builder(crag_id)
        url_data = get_data(crag_url)

        if url_data is None:
            print(f"Failed to get data for crag {crag_id}, skipping...")
            continue

        data_string = data_to_string(url_data)
        if data_string is None:
            print(f"Failed to process data for crag {crag_id}, skipping...")
            continue

        # Add debug output
        print(f"Data string preview for crag {crag_id}:")
        print(data_string[:200] if data_string else "None")

        climb_json = string_processor_climbs(data_string)
        if climb_json is None:
            print(f"Failed to extract climb data for crag {crag_id}, skipping...")
            continue

        # Add debug output
        print(f"JSON preview for crag {crag_id}:")
        print(climb_json[:200] if climb_json else "None")

        climb_json = string_processor_climbs(data_string)
        if climb_json is None:
            print(f"Failed to extract climb data for crag {crag_id}, skipping...")
            continue

        climb_dataframe = climbs_dataframe_creation(climb_json)
        if climb_dataframe is None:
            print(f"Failed to create dataframe for crag {crag_id}, skipping...")
            continue

        # Process the dataframe (add your processing logic here)
        print(f"Successfully processed crag {crag_id}")
        print(climb_dataframe)

        # Add delay to avoid overwhelming the server
        time.sleep(random.randint(2,10))


if __name__ == '__main__':
    main()
