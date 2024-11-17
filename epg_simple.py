from datetime import datetime, timedelta, tzinfo
import requests as r
import bs4
import polars as pl
import pytz

"""
for fetching the data - beware it's 60MB worth of data or so 
for development to be used once
worth to be updated once in 3 days
"""
fetch = True
"""
for parsing and writing data to csv - good to try different separator
"""
refresh = True
cet = pytz.timezone('CET')

if fetch:
    response = r.get("https://epg.ovh/pl.xml")
    with open("programy.xml", "w") as f:
        f.write(response.text)
if refresh:
    with open("programy.xml", "r") as f:
        soup = bs4.BeautifulSoup(f.read(), "xml")

    programmes = soup.find_all('programme')

    with open("programy.csv", "w") as f:
        f.write("kanał|tytuł|start|koniec|czas trwania\n")

    # Loop through each programme and extract required information
    for programme in programmes:
        channel = programme['channel']
        title = programme.title.string  # Extracting the title text

        # Extract start and stop attributes
        start_str = programme['start']
        stop_str = programme['stop']

        # Convert to UTC datetime objects
        start_time_utc = datetime.strptime(start_str, '%Y%m%d%H%M%S %z')
        stop_time_utc = datetime.strptime(stop_str, '%Y%m%d%H%M%S %z')

        # Convert UTC datetime objects to CET
        start_time_cet = start_time_utc.astimezone(cet)
        stop_time_cet = stop_time_utc.astimezone(cet)
        duration = (stop_time_cet - start_time_cet).total_seconds() / 60

        with open("programy.csv", "a") as f:
            f.write(f"{channel}|{title!r}|{start_time_cet}|{stop_time_cet}|{duration}\n")

with open("programy.csv", "r") as f:
    data = f.read()

current_time_utc = datetime.now(cet)

filtered = []
for row in data:
    channel, title, start, end, duration = row.split("|")
    datetime.strptime(start, "%d.%m %:H%M")
    datetime.strptime(end, "%d.%m %:H%M")

print(filtered)
