# -*- coding: utf-8 -*-
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException,StaleElementReferenceException,WebDriverException,ElementClickInterceptedException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
import re
import json
import time
import os
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import logging
from config import TWITTER_AUTH_TOKEN
from schedule import every , run_pending
import random
import platform
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TwitterExtractor:
    def __init__(self, headless=True):
        self.driver = self._start_chrome(headless)
        self.set_token()
        self.one_fectch_twitter_map = {}
        self.twitter_map = {}

    def _start_chrome(self, headless):
        options = Options()
        options.headless = headless
        if platform.system() == "Darwin":  # MacOS
            options.add_argument("--start-fullscreen")
        #else:
            #options.add_argument("--start-maximized")
        driver = webdriver.Chrome(options=options)
        driver.get("https://twitter.com")
        return driver

    def set_token(self, auth_token=TWITTER_AUTH_TOKEN):
        if not auth_token or auth_token == "YOUR_TWITTER_AUTH_TOKEN_HERE":
            raise ValueError("Access token is missing. Please configure it properly.")
        expiration = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
        cookie_script = f"document.cookie = 'auth_token={auth_token}; expires={expiration}; path=/';"
        self.driver.execute_script(cookie_script)

    def fetch_tweets(self, page_url, start_date, end_date):
        self.driver.get(page_url)
        cur_filename = f"data/tweets_{datetime.now().strftime('%Y-%m-%d')}"
        try:
            random_number = random.randint(10,20)
            wait = WebDriverWait(self.driver,random_number)
            following_tab = wait.until(EC.visibility_of_element_located((By.XPATH, "//span[text()='Following']")))
            following_tab.click()
            #following_tab = "//span[text()='Following']"
            #self.driver.find_element(By.XPATH, following_tab).click()
            logger.info("Navigated to 'Following' tab instead.")
            time.sleep(random_number)  # 等待 "Following" 标签加载
        except NoSuchElementException as e:
            logger.error(f"Error NoSuchElementException to 'Following' tab: {str(e)}")
            return  # 如果 "Following" 标签也不存在，退出方法
        except StaleElementReferenceException as e:
            logger.error(f"Error StaleElementReferenceException to 'Following' tab: {str(e)}")
            return
    
        # Convert start_date and end_date from "YYYY-MM-DD" to datetime objects
        #start_date = datetime.strptime(start_date, "%Y-%m-%d")
        #end_date = datetime.strptime(end_date, "%Y-%m-%d")
        daycount = 0
        first_count=0
        while True:
            tweet = self._get_first_tweet()
            if not tweet:
                if first_count > 3:
                    break
                first_count= first_count+1
                continue

            row = self._process_tweet(tweet)
            if row["date"]:
                try:
                    date = datetime.strptime(row["date"], "%Y-%m-%d %H")

                except ValueError as e:
                    # infer date format
                    logger.info(
                        f"Value error on date format, trying another format.{row['date']}",
                        e,
                    )
                    date = datetime.strptime(row["date"], "%d/%m/%Y")

                if date < start_date:
                    self._delete_first_tweet()
                    if row["is_pinned"]:
                        continue
                    else:
                        daycount = daycount + 1
                        if daycount > 5:
                            break
                elif date > end_date:
                    self._delete_first_tweet()
                    continue

            #如果推文的URL已经在twitter_map中，则删除第一个推文
            if row["url"] in self.twitter_map:
                self._delete_first_tweet()
                continue

            
            self.one_fectch_twitter_map[row["url"]] = row
            #self._save_to_json(row, filename=f"{cur_filename}.json")
            logger.info(
                f"Saving tweets...\n{row['date']},  {row['author_name']} -- {row['text'][:50]}...\n\n"
            )
            self._delete_first_tweet()

        # Save to Excel
        #self._save_to_excel(
        #    json_filename=f"{cur_filename}.json", output_filename=f"{cur_filename}.xlsx"
        #)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(TimeoutException),
    )
    def _get_first_tweet(
        self, timeout=10, use_hacky_workaround_for_reloading_issue=True
    ):
        try:
            # Wait for either a tweet or the error message to appear
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.find_elements(By.XPATH, "//article[@data-testid='tweet']")
                or d.find_elements(By.XPATH, "//span[contains(text(),'Try reloading')]")
            )

            # Check for error message and try to click "Retry" if it's present
            error_message = self.driver.find_elements(
                By.XPATH, "//span[contains(text(),'Try reloading')]"
            )
            if error_message and use_hacky_workaround_for_reloading_issue:
                logger.info(
                    "Encountered 'Something went wrong. Try reloading.' error.\nTrying to resolve with a hacky workaround (click on another tab and switch back). Note that this is not optimal.\n"
                )
                logger.info(
                    "You do not have to worry about data duplication though. The save to excel part does the dedup."
                )
                self._navigate_tabs()

                WebDriverWait(self.driver, timeout).until(
                    lambda d: d.find_elements(
                        By.XPATH, "//article[@data-testid='tweet']"
                    )
                )
            elif error_message and not use_hacky_workaround_for_reloading_issue:
                raise TimeoutException(
                    "Error message present. Not using hacky workaround."
                )

            else:
                # If no error message, assume tweet is present
                try:
                    return self.driver.find_element(
                        By.XPATH, "//article[@data-testid='tweet']"
                    )
                except NoSuchElementException:
                    logger.error("Could not find tweet")
                    return None

        except TimeoutException:
            logger.error("Timeout waiting for tweet or after clicking 'Retry'")
            return None
        except NoSuchElementException:
            logger.error("Could not find tweet or 'Retry' button")
            raise TimeoutException("NoSuchElementException")
        except ElementClickInterceptedException as e:
            logger.error("ElementClickInterceptedException")
            self.driver.execute_script("document.querySelector('.ad_close_button').click();")
            raise TimeoutException("ElementClickInterceptedException") 
        except WebDriverException as e:
            logger.error("WebDriverException")
            raise TimeoutException("NoSuchElementException") 
        



    def _navigate_tabs(self, target_tab="Following"):
        # Deal with the 'Retry' issue. Not optimal.
        try:
            # Click on the 'Media' tab
            random_number = random.randint(8, 20)
            wait = WebDriverWait(self.driver, random_number)
            clickable_element = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[text()='Media']")))
            clickable_element.click()
            #self.driver.find_element(By.XPATH, "//span[text()='Media']").click()
            time.sleep(random_number)  # Wait for the Media tab to load

            # Click back on the Target tab. If you are fetching posts, you can click on 'Posts' tab
            wait = WebDriverWait(self.driver, random_number)
            clickable_element = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[text()='Following']")))
            clickable_element.click()

            #self.driver.find_element(By.XPATH, f"//span[text()='{target_tab}']").click()
            time.sleep(random_number)  # Wait for the Likes tab to reload
        except ( NoSuchElementException,TimeoutException) as e:
            logger.error("Error navigating tabs: " + str(e))
        # 如果 "Media" 标签不存在或点击失败，尝试点击 "Following" 标签
            try:
                following_tab = "//span[text()='For You']"
                self.driver.find_element(By.XPATH, following_tab).click()
                logger.info("Navigated to 'For you' tab instead.")
                time.sleep(random_number)  # 等待 "Following" 标签加载
            except NoSuchElementException as e:
                logger.error(f"Error navigating to 'Following' tab: {str(e)}")
                return  # 如果 "Following" 标签也不存在，退出方法

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(1))
    def _process_tweet(self, tweet):

        author_name, author_handle = self._extract_author_details(tweet)
        try:
            data = {
                "text": self._get_element_text(
                    tweet, ".//div[@data-testid='tweetText']"
                ),
                "author_name": author_name,
                "author_handle": author_handle,
                "date": self._get_element_attribute(tweet, "time", "datetime")[:13] or "",
                "lang": self._get_element_attribute(
                    tweet, "div[data-testid='tweetText']", "lang" or None
                ),
                "url": self._get_tweet_url(tweet),
                "mentioned_urls": self._get_mentioned_urls(tweet),
                "is_retweet": self.is_retweet(tweet),
                "media_type": self._get_media_type(tweet),
                "images_urls": (
                    self._get_images_urls(tweet)
                    if self._get_media_type(tweet) == "Image"
                    else None
                ),
                "is_pinned": self.is_pinned(tweet),
            }
        except Exception as e:
            logger.error(f"Error processing tweet: {e}")
            logger.info(f"Tweet: {tweet}")
            raise
        # Convert date format
        if data["date"]:
            data["date"] = datetime.strptime(data["date"], "%Y-%m-%dT%H").strftime(
                "%Y-%m-%d %H"
            )

        # Extract numbers from aria-labels
        data.update(
            {
                "num_reply": self._extract_number_from_aria_label(tweet, "reply"),
                "num_retweet": self._extract_number_from_aria_label(tweet, "retweet"),
                "num_like": self._extract_number_from_aria_label(tweet, "like"),
            }
        )
        return data

    def _get_element_text(self, parent, selector):
        try:
            return parent.find_element(By.XPATH, selector).text
        except (NoSuchElementException,StaleElementReferenceException) as e:
            logger.error(f"Element not found: {selector}")
            return ""
        except Exception as e:
        # 捕获其他可能的异常，并记录错误
            logger.error(f"Error while getting text from element: {selector}, Error: {str(e)}")
            return ""

    def _get_element_attribute(self, parent, selector, attribute):
        try:
            return parent.find_element(By.CSS_SELECTOR, selector).get_attribute(
                attribute
            )
        except (NoSuchElementException,StaleElementReferenceException) as e:
            logger.error(f"Error getting attribute '{attribute}' from element with selector '{selector}': {e}")
    
            return ""

    def _get_mentioned_urls(self, tweet):
        try:
            # Find all 'a' tags that could contain links. You might need to adjust the selector based on actual structure.
            link_elements = tweet.find_elements(
                By.XPATH, ".//a[contains(@href, 'http')]"
            )
            urls = [elem.get_attribute("href") for elem in link_elements]
            return urls
        except (NoSuchElementException,StaleElementReferenceException) as e:
            return []

    def is_retweet(self, tweet):
        try:
            # This is an example; the actual structure might differ.
            retweet_indicator = tweet.find_element(
                By.XPATH, ".//div[contains(text(), 'Retweeted')]"
            )
            if retweet_indicator:
                return True
        except (NoSuchElementException,StaleElementReferenceException) as e:
            return False
    def is_pinned(self,tweet):
        try:
            retweet_pinned = tweet.find_element(
                By.XPATH, ".//span[contains(text(), 'Pinned')]"
            )
            if retweet_pinned:
                return True 
        except (NoSuchElementException,StaleElementReferenceException) as e:
            return False 
    def _get_tweet_url(self, tweet):
        try:
            link_element = tweet.find_element(
                By.XPATH, ".//a[contains(@href, '/status/')]"
            )
            return link_element.get_attribute("href")
        except (NoSuchElementException,StaleElementReferenceException) as e:
            return ""

    def _extract_author_details(self, tweet):
        author_details = self._get_element_text(
            tweet, ".//div[@data-testid='User-Name']"
        )

        if not author_details:
            # 如果没有获取到作者详情，可能是因为元素未找到或其他错误
            logger.warning("Failed to extract author details. The element might be missing or another error occurred.")
            return "", "" 
    
        # Splitting the string by newline character
        parts = author_details.split("\n")
        if len(parts) >= 2:
            author_name = parts[0]
            author_handle = parts[1]
        else:
            # Fallback in case the format is not as expected
            author_name = author_details
            author_handle = ""

        return author_name, author_handle

    def _get_media_type(self, tweet):
        try:
            if tweet.find_elements(By.CSS_SELECTOR, "div[data-testid='videoPlayer']"):
                return "Video"
            if tweet.find_elements(By.CSS_SELECTOR, "div[data-testid='tweetPhoto']"):
                return "Image"
            return "No media"
        except (NoSuchElementException,StaleElementReferenceException) as e:
            return "No media" 

    def _get_images_urls(self, tweet):
        images_urls = []
        try:
            images_elements = tweet.find_elements(
                By.XPATH, ".//div[@data-testid='tweetPhoto']//img"
            )
            for image_element in images_elements:
                images_urls.append(image_element.get_attribute("src"))
            return images_urls
        except (NoSuchElementException,StaleElementReferenceException) as e:
            return "No urls"

    def _extract_number_from_aria_label(self, tweet, testid):
        try:
            text = tweet.find_element(
                By.CSS_SELECTOR, f"div[data-testid='{testid}']"
            ).get_attribute("aria-label")
            numbers = [int(s) for s in re.findall(r"\b\d+\b", text)]
            return numbers[0] if numbers else 0
        except (NoSuchElementException,StaleElementReferenceException) as e:
            return 0

    def _delete_first_tweet(self, sleep_time_range_ms=(0, 1000)):
        try:
            tweet = self.driver.find_element(
                By.XPATH, "//article[@data-testid='tweet'][1]"
            )
            self.driver.execute_script("arguments[0].remove();", tweet)
        except (NoSuchElementException,StaleElementReferenceException) as e:
            logger.info("Could not find the first tweet to delete.")

    @staticmethod
    def _save_to_json(data, filename="data.json"):
        with open(filename, "a", encoding="utf-8") as file:
            json.dump(data, file,ensure_ascii=False)
            file.write("\n")

    @staticmethod
    def _save_to_excel(json_filename, output_filename="data/data.xlsx"):

            # 检查JSON文件是否存在
        if not os.path.exists(json_filename):
            logger.info(f"File {json_filename} does not exist. Skipping.")
            return
    
        # Read JSON data
        cur_df = pd.read_json(json_filename, lines=True)

        # Drop duplicates & save to Excel
        cur_df.drop_duplicates(subset=["url"], inplace=True)
        cur_df.to_excel(output_filename, index=False)
        logger.info(
            f"\n\nDone saving to {output_filename}. Total of {len(cur_df)} unique tweets."
        )

def boyer_moore(text, pattern):
    # Boyer-Moore string search algorithm
    # 构建坏字符表
    bad_char_table = {}
    for i in range(len(pattern) - 1):
        bad_char_table[pattern[i]] = len(pattern) - 1 - i

    # 构建好后缀表
    good_suffix_table = [len(pattern)] * len(pattern)
    for i in range(len(pattern) - 1):
        suffix = pattern[i + 1:]
        for j in range(len(pattern) - 1, i, -1):
            if suffix[j - i - 1] != pattern[j]:
                break
            good_suffix_table[j] = len(pattern) - 1 - j + i

    # 匹配过程
    i = len(pattern) - 1
    while i < len(text):
        j = len(pattern) - 1
        while j >= 0 and text[i - j] == pattern[j]:
            j -= 1
        if j < 0:
            return i
        else:
            i += max(bad_char_table.get(text[i], len(pattern)), good_suffix_table[j])

        return -1
    

class TrieNode:
    def __init__(self):
        self.children = {}
        self.fail = None  # 失败指针
        self.output = []  # 存储在此节点结束的所有字典词

class AhoCorasick:
    def __init__(self):
        self.root = TrieNode()

    def add_word(self, word):
        node = self.root
        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.output.append(word)

    def build_failure_pointers(self):
        queue = []
        for child in self.root.children.values():
            child.fail = self.root
            queue.append(child)
        while queue:
            current_node = queue.pop(0)
            for char, child_node in current_node.children.items():
                queue.append(child_node)
                fail_node = current_node.fail
                while fail_node is not None and char not in fail_node.children:
                    fail_node = fail_node.fail
                child_node.fail = fail_node.children[char] if fail_node else self.root
                child_node.output += child_node.fail.output

    def is_word_boundary(self, text, pos, pattern_len):
        """
        检查给定位置的匹配是否在单词边界上。
        """
        start_boundary = pos == 0 or text[pos - 1].isspace() or not text[pos - 1].isalpha()
        end_boundary = (pos + pattern_len) == len(text) or text[pos + pattern_len].isspace() or not text[pos + pattern_len].isalpha()
        return start_boundary and end_boundary
    
    def search(self, text):
        node = self.root
        for i, char in enumerate(text):
            while node is not None and char not in node.children:
                node = node.fail
            if node is None:
                node = self.root
                continue
            node = node.children[char]
            if node.output:
                 for pattern in node.output:
                    if self.is_word_boundary(text, i - len(pattern) + 1, len(pattern)):
                        print(f"Found '{pattern}' at position {i - len(pattern) + 1}")
                        return True
        return False
              
class CryptocurrencyFileManager:
    def __init__(self,file_path):
        self.file_path = file_path
        self.json_data = None
        self.combined_list = []

    def read_json_data(self):
        try:
            print(os.getcwd())
            with open(self.file_path,"r") as file:
                self.json_data = json.load(file)
                return self.json_data
        except FileNotFoundError as e:
            logger.error(f"File not found: {self.file_path}")
            return None
        
    def parse_data(self):
        for item in self.json_data["data"]:
            self.combined_list.append(item["name"])
            self.combined_list.append(item["symbol"])
        return self.combined_list
    

class Lark:
    def __init__(self) -> None:
        self.url = "https://open.larksuite.com/open-apis/bot/v2/hook/f15c4239-2622-40e4-94dd-da4f934be668"
        self.content_type = "application/json"
        self.context = None

    def send_message(self, message):
        data = {
            "msg_type": "text",
            "content": {
                "text": message
            }
        }
        
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.1,status_forcelist=[500,502,503,504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        try:
            response = session.post(self.url, headers={"Content-Type": self.content_type}, json=data)
            return response.json()
        except requests.exceptions.ConnectionError as e:
            logger.error(" request failed: ", e)
            return None
    
    

if __name__ == "__main__":
    
    crypto = CryptocurrencyFileManager("cpytocurrency.json")
    crypto.read_json_data()
    crypto.parse_data()

    ac = AhoCorasick()
    for word in crypto.combined_list:
        ac.add_word(word)
    ac.build_failure_pointers()

    scraper = TwitterExtractor()

    lark = Lark()
    while True:
        now = datetime.now()
        now = now.replace(minute=0, second=0, microsecond=0)
        start_date_hour = now - timedelta(hours=24)

        scraper.fetch_tweets(
            "https://twitter.com/home",
            start_date_hour,
            now,
        )

        for key,value in scraper.one_fectch_twitter_map.items():
            found = ac.search(value["text"])
            if found:
                scraper.twitter_map[key] = value
                # Save to JSON
                scraper._save_to_json(value, filename=f"data/tweets_{now.strftime('%Y-%m-%d')}.json")
                # Send message to Lark
                json_data = json.dumps(value, ensure_ascii=False, indent=4)
                lark.send_message(json_data)
        random_number = random.randint(100, 300)
        time.sleep(random_number)
        scraper.one_fectch_twitter_map.clear()

    # If you just want to export to Excel, you can use the following line
    # scraper._save_to_excel(json_filename="tweets_2024-02-01_14-30-00.json", output_filename="tweets_2024-02-01_14-30-00.xlsx")
