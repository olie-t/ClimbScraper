import requests
from bs4 import BeautifulSoup as bs
import time
import random
from typing import Optional
from climb_scraper.data.database import ClimbingDatabase


def is_valid_crag_page(content):
    soup = bs(content, 'html.parser')
    title = soup.title.string if soup.title else ''
    return 'UKC Logbook - ' in title


def get_random_user_agent() -> str:
    """Return a random user agent string"""
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0'
    ]
    return random.choice(user_agents)


def check_crag_id(crag_id: int) -> bool:
    """Check if a specific crag ID is valid"""
    headers = {
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }

    try:
        response = requests.get(
            f"https://www.ukclimbing.com/logbook/crag.php?id={crag_id}",
            headers=headers
        )
        return response.status_code == 200 and is_valid_crag_page(response.content)
    except Exception as e:
        print(f"Error checking crag ID {crag_id}: {e}")
        return False


class CragFinder:
    def __init__(self, db: ClimbingDatabase, max_attempts: int = 50):
        self.db = db
        self.MAX_ATTEMPTS = max_attempts
        self.errors_in_a_row = 0

    def get_last_checked_id(self) -> int:
        """Get the highest crag ID we've checked"""
        try:
            conn = self.db.connect()
            if conn:
                cursor = conn.cursor()
                cursor.execute("SELECT MAX(crag_id) FROM crag_ids")
                result = cursor.fetchone()[0]
                self.db.close()
                return result if result else 0
        except Exception as e:
            print(f"Error getting last checked ID: {e}")
            return 0

    def find_new_crags(self):
        """Find new valid crag IDs and add them to the database"""
        last_id = self.get_last_checked_id()
        next_id = last_id + 1
        valid_ids = []
        self.errors_in_a_row = 0

        print(f"Starting search from crag ID: {next_id}")

        while self.errors_in_a_row < self.MAX_ATTEMPTS:
            print(f"Checking crag ID: {next_id}")

            # Add random delay between checks
            time.sleep(random.uniform(3, 7))

            if check_crag_id(next_id):
                print(f"Found valid crag ID: {next_id}")
                valid_ids.append(next_id)
                self.errors_in_a_row = 0

                # Add to database immediately
                self.db.add_crag_ids(next_id, next_id)

            else:
                print(f"Invalid crag ID: {next_id}")
                self.errors_in_a_row += 1

            next_id += 1

        print(f"Search complete. Found {len(valid_ids)} new crag IDs")
        return valid_ids


def populate_ids_list(db: ClimbingDatabase):
    """Main function to find new crag IDs"""
    finder = CragFinder(db)
    return finder.find_new_crags()