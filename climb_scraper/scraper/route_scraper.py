import random

from climb_scraper.scraper.scraper_functions import get_crag_id, url_builder, get_data, data_to_string, \
    string_processor_climbs, climbs_dataframe_creation, string_processor_grades, grades_dataframe_creation, \
    climbs_grades_dataframe_merge
from data.database import ClimbingDatabase
from scraper.list_builder import populate_ids_list
import time

def import_ids_from_file(db: ClimbingDatabase, filename: str) -> bool:
    """Import crag IDs from existing text file into database"""
    try:
        with open(filename, 'r') as f:
            ids = [int(line.strip()) for line in f if line.strip()]
        if ids:
            min_id = min(ids)
            max_id = max(ids)
            return db.add_crag_ids(min_id, max_id)
    except Exception as e:
        print(f"Error importing IDs from file: {e}")
        return False


def main():
    # Initialize the database
    db = ClimbingDatabase("climbing_data.db")
    # First run: Import existing IDs from text file if needed
    import_ids_from_file(db, 'crag_ids.txt')

    failed_attempts = {}
    MAX_RETRIES = 3

    while True:
        # Get processing statistics
        processed, unprocessed = db.get_processing_stats()
        print(f"Processing status: {processed} crags processed, {unprocessed} remaining")

        if unprocessed == 0:
            print("No unprocessed crags found. Looking for new crags...")
            populate_ids_list(db)  # This will add new crag IDs to the database
            continue  # Go back to the start of the loop

        # Get next crag to process
        crag_id = db.get_next_unprocessed_crag_id()
        if crag_id is None:
            print("No unprocessed crags found.")
            break

        print(f"Processing crag ID: {crag_id}")

        # Check if we've tried this crag too many times
        if failed_attempts.get(crag_id, 0) >= MAX_RETRIES:
            print(f"Crag {crag_id} has failed {MAX_RETRIES} times, marking as permanently failed")
            db.mark_crag_processed(crag_id, success=True)
            continue

        url_data = get_data(url_builder(crag_id))

        if url_data is None:
            failed_attempts[crag_id] = failed_attempts.get(crag_id, 0) + 1
            if failed_attempts[crag_id] >= MAX_RETRIES:
                print(f"Failed to get data for crag {crag_id} after {MAX_RETRIES} attempts, marking as failed")
                db.mark_crag_processed(crag_id, success=True)
            continue

        data_string = data_to_string(url_data)
        if data_string is None:
            failed_attempts[crag_id] = failed_attempts.get(crag_id, 0) + 1
            continue

        climb_json = string_processor_climbs(data_string)
        climb_dataframe = climbs_dataframe_creation(climb_json)

        if climb_dataframe is None:
            failed_attempts[crag_id] = failed_attempts.get(crag_id, 0) + 1
            continue

        # Successfully processed the crag
        if db.insert_crag(crag_id) and db.insert_climbs(climb_dataframe, crag_id):
            print(f"Successfully processed crag {crag_id}")
            db.mark_crag_processed(crag_id, success=True)
            # Clear any failed attempts for this crag
            failed_attempts.pop(crag_id, None)
        else:
            failed_attempts[crag_id] = failed_attempts.get(crag_id, 0) + 1

        # Insert data into database
        if db.insert_crag(crag_id) and db.insert_climbs(climb_dataframe, crag_id):
            print(f"Successfully processed crag {crag_id}")
            db.mark_crag_processed(crag_id, success=True)
        else:
            print(f"Failed to store data for crag {crag_id}")
            db.mark_crag_processed(crag_id, success=False)

        # Add delay to avoid overwhelming the server
        base_delay = random.uniform(2, 5)  # Base delay between 2-5 seconds
        if random.random() < 0.1:  # 10% chance of a longer delay
            base_delay += random.uniform(5, 15)  # Add 5-15 seconds occasionally
        time.sleep(base_delay)


if __name__ == '__main__':
    main()