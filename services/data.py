import os
from datetime import datetime

import bs4
import lxml.etree
import polars as pl
import pytz
import requests as r
import logging

import configuration
import environment
from environment import Environment

logging.basicConfig(filename="data_processing.log",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)


timezone = pytz.timezone(configuration.timezone)


class ProgrammeData:

    def __init__(self):
        self.data_frame: pl.Dataframe = pl.DataFrame()

    async def refresh_data(self, force: bool = False):
        if configuration.environment != Environment.LOCAL or force:
            logger.info("Fetching data from epg...")
            response = r.get("https://epg.ovh/pl.xml")
            content_length_bytes = len(response.content)
            content_length_mb = content_length_bytes / (1024 * 1024)
            logger.info(f"Data fetched successfully")
            logger.info(f"Response size: {content_length_mb:.2f} MB")
            # xml = lxml.etree.fromstring(response.content)

            logger.info("Parsing the data...")
            with open("programy.xml", "w", encoding="utf-8") as f:
                f.write(response.text)
            with open("programy.xml", "r", encoding="utf-8") as f:
                xml = lxml.etree.fromstring(f.read().encode('utf-8'))
            logger.info("Parsing the data completed")

            logger.info("Preparing the data...")
            programmes = xml.xpath('//programme')
            with open("programy.csv", "w") as f:
                f.write("kanał|tytuł|start|koniec|czas trwania\n")

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
                # start_time_cet = timezone.localize(start_time_utc)
                # stop_time_cet = timezone.localize(stop_time_utc)
                duration = (stop_time_cet - start_time_cet).total_seconds() / 60

                with open("programy.csv", "a") as f:
                    f.write(f"{channel}|{title!r}|{start_time_cet}|{stop_time_cet}|{duration}\n")

        self.data_frame = pl.read_csv("programy.csv", separator="|", truncate_ragged_lines=True)
        self.data_frame = self.data_frame.with_columns(
            pl.col('start').str.strptime(pl.Datetime),
            pl.col('koniec').str.strptime(pl.Datetime),
        )
        self.data_frame = self.data_frame.with_columns(
            pl.col('start').dt.convert_time_zone('Poland'),
            pl.col('koniec').dt.convert_time_zone('Poland'),
        )
        logger.info("Preparing the data completed.")


programme_data = ProgrammeData()
