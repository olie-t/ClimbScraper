import pandas as pd
from bs4 import BeautifulSoup as bs
import requests
import json

with open("crag_ids.txt", "r") as crag_file:
    crag_id = crag_file.readline()

site = (f"https://www.ukclimbing.com/logbook/crag.php?id={crag_id}")
results = requests.get(site)
soup = bs(results.content, 'html.parser')
links = soup.find_all('a')

for link in links:
    href = link.get('href')
    text = link.get_text()
    print(f"LINK: {href}\nTEXT: {text}")
