import asyncio
import copy
import os
from datetime import datetime

import lxml.etree
import polars as pl
import pytz
import requests as r
import logging

from pandas.io.stata import excessive_string_length_error
from pyarrow.util import download_tzdata_on_windows

import configuration
from configuration import environment
from environment import Environment

logging.basicConfig(format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

timezone = pytz.timezone(configuration.timezone)


class ProgrammeData:

    def __init__(self):
        self.data_frame: pl.Dataframe = pl.DataFrame()

    async def download_data(self, file_path: str):
        logger.info("Fetching data from epg...")
        response = r.get("https://epg.ovh/pl.xml")

        if response.status_code != 200:
            logger.error("Failed to fetch data.")
            return

        content_length_bytes = len(response.content)
        content_length_mb = content_length_bytes / (1024 * 1024)
        logger.info(f"Data fetched successfully. Response size: {content_length_mb:.2f} MB")
        with open(file_path, "w") as f:
            f.write(response.content)
        # return response.content

    async def parse_data(self, source_xml_file_name: str,
                         csv_destination_file_name: str):
        logger.info("Parsing the data...")
        # Define today's date in the required format
        today_date = datetime.now(pytz.timezone(configuration.timezone)).strftime('%Y%m%d')
        programmes = []

        # Use iterparse for memory-efficient parsing
        for event, elem in lxml.etree.iterparse(source_xml_file_name, events=('end',)):
            if elem.tag == 'programme':
                start_time = elem.attrib.get('start', '')
                # Check if the start attribute contains today's date
                if start_time.startswith(today_date):
                    programmes.append(copy.deepcopy(elem))
                # Clear the element to save memory
                elem.clear()

        logger.info("Preparing the data...")

        # Use a list to collect rows instead of writing to a file in a loop
        csv_rows = ["kanał|tytuł|start|koniec|czas trwania"]

        for programme in programmes:
            channel = programme.attrib['channel']
            title = programme.find('title').text
            start_str = programme.attrib['start']
            stop_str = programme.attrib['stop']

            start_time_utc = datetime.strptime(start_str, '%Y%m%d%H%M%S %z')
            stop_time_utc = datetime.strptime(stop_str, '%Y%m%d%H%M%S %z')

            # Convert UTC datetime objects to CET
            start_time_cet = start_time_utc.astimezone(timezone)
            stop_time_cet = stop_time_utc.astimezone(timezone)
            duration = (stop_time_cet - start_time_cet).total_seconds() / 60

            # Append formatted string to list instead of writing to file directly
            csv_rows.append(f"{channel}|{title!r}|{start_time_cet}|{stop_time_cet}|{duration}")

            # Write all rows to CSV at once
        with open(csv_destination_file_name, "w", encoding="utf-8") as f:
            f.write("\n".join(csv_rows))

        # Read the CSV into a DataFrame with Polars
        self.data_frame = pl.read_csv(csv_destination_file_name, separator="|", truncate_ragged_lines=True)

        # Convert date strings to datetime objects in a single operation
        self.data_frame = self.data_frame.with_columns(
            [
                pl.col('start').str.strptime(pl.Datetime),
                pl.col('koniec').str.strptime(pl.Datetime),
            ]
        ).with_columns(
            [
                pl.col('start').dt.convert_time_zone(configuration.timezone),
                pl.col('koniec').dt.convert_time_zone(configuration.timezone),
            ]
        )

        logger.info("Preparing the data completed.")

    async def refresh_data(self, xml_file_path: str, csv_file_path: str, force: bool = False):
        if configuration.environment != Environment.LOCAL or force:
            await self.download_data(xml_file_path)
        await self.parse_data(xml_file_path, csv_file_path)


programme_data = ProgrammeData()

if __name__ == '__main__':
    asyncio.run(programme_data.refresh_data("../programy.xml", "../programy.csv"))
