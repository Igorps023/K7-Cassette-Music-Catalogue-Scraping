import pandas as pd
import os
import re
from datetime import datetime

from selenium.webdriver import Firefox
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class Scraping:

    def __init__(self, url=None) -> None:
        """Class for webscraping and data treatment"""
        self.url = url
        self.driver = None

    def selenium_driver_starter(self):
        """Starts a webbrowser and access webpage and runs until the initial scraping page"""
        if self.url:
            options = Options()
            # options.add_argument("--headless")
            self.driver = Firefox(options=options)
            self.driver.get(self.url)
            self.driver.implicitly_wait(5)
            return self.driver
        else:
            raise ValueError("URL is missing")

    def selenium_driver_close(self):
        """Closes the webdriver"""
        if self.driver:
            self.driver.close()
        else:
            raise ValueError("Cannot close a driver that didnt start")

    def selenium_starter_K7(self):
        """Access webpage and runs until the initial scraping page"""
        if not self.driver:
            raise ValueError("Requires URL to start driver")
        # Goes to the cassette catalogue
        see_more_items = self.driver.find_elements(
            By.CSS_SELECTOR, "a.pull-right.see-more"
        )
        k7 = [j for j in see_more_items if "k7" in j.get_attribute("href")]
        if k7:
            k7[0].click()
            self.driver.implicitly_wait(5)
        else:
            raise ValueError("No K7 element found to click")

    def scrape_data(self, dt_proc_full):
        """Collects pages infos and returns a dictionary"""
        if self.driver:
            page_catalogue_items = self.driver.find_element(
                By.CSS_SELECTOR, "div.list-container"
            )
            product_items = page_catalogue_items.find_elements(
                By.CSS_SELECTOR, "div.list-item"
            )

            page_dictionary = {}
            for index, j in enumerate(product_items):
                full_title = j.find_element(By.CSS_SELECTOR, "a").get_attribute("title")
                artist = full_title.split("·")[0].strip()
                title = j.find_element(By.CSS_SELECTOR, "span.title").text
                price = j.find_element(
                    By.XPATH,
                    './/button[contains(@class, "btn") and contains(@class, "price")]',
                ).text
                media_format = j.find_element(By.CSS_SELECTOR, "acronym").text
                image = j.find_element(By.CSS_SELECTOR, "img").get_attribute("src")
                direct_link = j.find_element(By.CSS_SELECTOR, "a").get_attribute("href")

                # Dictionary store process
                page_dictionary[artist] = {
                    "artist": artist,
                    "full_title": full_title,
                    "title": title,
                    "price": price,
                    "media_format": media_format,
                    "image": image,
                    "direct_link": direct_link,
                    "dt_proc_full": dt_proc_full,
                }

            return page_dictionary
        else:
            raise ValueError("Driver not declared")

    def selenium_scrape_loop(self):
        # If the element disabled exists, means that the back button is disable
        # It means that you cant return to a previous page, because you are already in the 1st page
        # Code Logic: Scrape then click nextpage

        before_page_btn_disabled = self.driver.find_elements(
            By.XPATH,
            '//button[@class="btn btn-primary btn-sm" and contains(@title, "Anterior") and @disabled="disabled"]',  # and @disabled="disabled"
        )

        if before_page_btn_disabled:
            cassete_catalogue = {}
            dt_proc_full = self.get_timestamp_full()
            cassete_catalogue.update(self.scrape_data(dt_proc_full))

            while True:
                # Next Page button exists, clicks and scrape again
                next_page_btn = self.driver.find_elements(
                    By.XPATH,
                    '//a[@class="btn btn-primary btn-sm" and contains(@title, "Próximo")]',
                )

                # If theres no next button, scrape data and break the loop
                if not next_page_btn:
                    cassete_catalogue.update(self.scrape_data(dt_proc_full))

                    break

                next_page_btn[0].click()

                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "div.list-container")
                    )
                )

                cassete_catalogue.update(self.scrape_data(dt_proc_full))

        return cassete_catalogue

    def convert_df_save_parquet(self, cassete_catalogue):
        """Receives a dictionary, converts to dataframe and then saves it as a parquet file"""

        if not cassete_catalogue:
            raise ValueError("No full dictionary to store in a dataframe")

        # Converts dict to df and treat artists name
        df = pd.DataFrame.from_dict(cassete_catalogue, orient="index").reset_index()
        df["artist"] = df["artist"].apply(self.preprocess_artist_name)
        df["artist"] = df["artist"].apply(
            lambda x: self.extract_text_before_substring(x)
        )
        df["artist"] = df["artist"].str.replace(r"_+", "_")

        # Create dir
        dt_proc_full = self.get_timestamp_full()
        directory = f"./data_selenium/{dt_proc_full}/"
        os.makedirs(directory, exist_ok=True)

        df.to_parquet(
            os.path.join(directory, "catalogue.parquet"), partition_cols=["artist"]
        )

        print(f"Saved to {directory}")

    @staticmethod
    def get_timestamp_full():
        """Get current timestamp in the format yyyymmdd_hhmmss_fff (with milliseconds)"""
        return datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]

    @staticmethod
    def get_timestamp_daily():
        """Get current timestamp in the format yyyymmdd"""
        return datetime.now().strftime("%Y%m%d")

    @staticmethod
    def get_timestamp_hourly():
        """Get current timestamp in the format hhmmssfff (hour, min, sec, milisec)"""
        return datetime.now().strftime("%H%M%S%f")[:-3]

    @staticmethod
    def preprocess_artist_name(name):
        """Replace spaces and special characters with underscores"""
        name = re.sub(r"[^a-zA-Z0-9]", "_", name)
        return name

    @staticmethod
    def extract_text_before_substring(text, substring="__cassette"):
        """Convert the input text to lowercase and find the position of the substring"""
        pos = text.lower().find(substring)
        # If the substring is found (pos is not -1)
        if pos != -1:
            # Return the part of the text before the substring
            return text[:pos]
        # If the substring is not found, return the original text
        return text


scraper = Scraping("https://imusic.br.com/")
scraper.selenium_driver_starter()
scraper.selenium_starter_K7()
cassete_catalogue = scraper.selenium_scrape_loop()
scraper.convert_df_save_parquet(cassete_catalogue)
scraper.selenium_driver_close()
