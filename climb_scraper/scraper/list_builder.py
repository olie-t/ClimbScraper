#import pandas as pd
from bs4 import BeautifulSoup as bs
import requests
import time

def is_valid_crag_page(content):
    soup = bs(content, 'html.parser')
    title = soup.title.string if soup.title else ''
    return 'UKC Logbook - ' in title

with open('crag_ids.txt', 'r') as f:
    lines = f.read().splitlines()
    last_line = lines[-1]

KNOWNID = int(last_line)
MAX_ATTEMPTS = 50


valid_ids = [KNOWNID]
errors_in_a_row = 0
next_id = valid_ids[-1]
if __name__ == '__main__':
    for _ in range(MAX_ATTEMPTS):
        next_id += 1
        response = requests.get(f"https://www.ukclimbing.com/logbook/crag.php?id={next_id}")

        if response.status_code == 200 and is_valid_crag_page(response.content):
            valid_ids.append(next_id)
            errors_in_a_row = 0
            print(f"This id is valid {next_id}")

        else:
            errors_in_a_row += 1
            print(f'This id is not valid {next_id}')

        if errors_in_a_row > 50:
            break
    print(valid_ids)
    crag_file = open('crag_ids.txt', 'a')
    for id in valid_ids:
        crag_file.write(str(id) +'\n')
    crag_file.close()


