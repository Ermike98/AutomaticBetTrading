from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import selenium
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException, ElementNotInteractableException
from selenium.webdriver.chrome.options import Options
import datetime
import time
import pandas as pd
from scrapers.site_scraper import SiteScraper
from collections import defaultdict
from utility.bet_utility import BetPrice
from dask.distributed import Client
import math
from utility.string_utility import clean_club_name


def parse_1x2_button(row):
    # Find odds
    odds_div = row.find('div', class_='odds')
    if not odds_div:
        return {}
    odds = odds_div.find_all('label')
    # Find liquidity
    liquidity = row.find('div', class_='odds').find_all('span')
    if not (odds and len(odds) == 6 and liquidity and len(liquidity) == 8):
        return {}
    return {'1': BetPrice(float(odds[0].text),
                          float(liquidity[1].text[:-1]),
                          float(odds[3].text),
                          float(liquidity[5].text[:-1])),
            'x': BetPrice(float(odds[1].text),
                          float(liquidity[2].text[:-1]),
                          float(odds[4].text),
                          float(liquidity[6].text[:-1])),
            '2': BetPrice(float(odds[2].text),
                          float(liquidity[3].text[:-1]),
                          float(odds[5].text),
                          float(liquidity[7].text[:-1]))}

def parse_12_button(row):
    # Find odds
    odds_div = row.find('div', class_='odds')
    if not odds_div:
        return {}
    odds = odds_div.find_all('label')
    # Find liquidity
    liquidity = row.find('div', class_='odds').find_all('span')
    # print(liquidity)
    # print(odds)
    if not (odds and len(odds) == 4 and liquidity and len(liquidity) == 6):
        return {}
    return {'1': BetPrice(float(odds[0].text),
                          float(liquidity[1].text[:-1]),
                          float(odds[2].text),
                          float(liquidity[4].text[:-1])),
            '2': BetPrice(float(odds[1].text),
                          float(liquidity[2].text[:-1]),
                          float(odds[3].text),
                          float(liquidity[5].text[:-1]))}

def parse_uo_button(row):
    # Find odds
    odds_div = row.find('div', class_='odds')
    if not odds_div:
        return {}
    odds = odds_div.find_all('label')
    # Find liquidity
    liquidity = row.find('div', class_='odds').find_all('span')
    if not (odds and len(odds) == 4 and liquidity and len(liquidity) == 6):
        return {}
    # Convert strings to numbers
    try:
        odds = list(map(lambda x: float(x.text), odds))
        liquidity = list(map(lambda x: float(x.text[:-1]), liquidity))
    except ValueError:
        # If any of these is not numeric return an empty dict
        return {}
    return {'u': BetPrice(odds[0],
                          liquidity[1],
                          odds[2],
                          liquidity[4]),
            'o': BetPrice(odds[1],
                          liquidity[2],
                          odds[3],
                          liquidity[5])}

def parse_uox_button(bet_type):
    x = bet_type[2:]
    def f(row):
        bets = parse_uo_button(row)
        if 'u' in bets:
            if 'o' in bets:
                return {'u'+x: bets['u'], 'o'+x: bets['o']}
            return {'u'+x: bets['u']}
        if 'o' in bets:
            return {'o' + x: bets['o']}
        return {}
    return f

def parse_row(row_html, bet_type='1x2'):
    row = BeautifulSoup(row_html, 'html.parser')
    # print(row.text)
    # print('bet type:', bet_type)
    if bet_type == '1x2':
        bet_type_bets_parser = parse_1x2_button
    elif bet_type == '12':
        bet_type_bets_parser = parse_12_button
    elif bet_type.startswith('uo'):
        bet_type_bets_parser = parse_uox_button(bet_type)
    else:
        raise Exception(f"bet_type {bet_type} is not allowed!")
    # campionato = row_div.find('div', class_='details').text

    # Find match date
    # matchDate = str(pd.to_datetime(row_div.find('div', class_='date')['dt']).date())
    matchDate = row.find('div', class_='date')['dt'][:10]

    # Find clubs
    clubs = row.find('a', class_='da').find_all('b')
    if len(clubs) != 2:
        return (None, None)
    club1_tag, club2_tag = clubs
    clubs_name = [clean_club_name(club1_tag.text),
                  clean_club_name(club2_tag.text)]
    clubs_name.sort()
    сlub1, сlub2 = clubs_name

    # Return the bet tuple
    return (сlub1, сlub2, matchDate), bet_type_bets_parser(row)


class BetflagScraper(SiteScraper):
    def __init__(self, sport='calcio', bet_type='1x2', max_additional_data=-1, cluster=None,
                 offline=True, live=True, headless=True):
        if max_additional_data == -1:
            max_additional_data = math.inf
        self.max_additional_data = max_additional_data
        self.n_additional_data_loaded = [max_additional_data, max_additional_data]
        self.sport = 'calcio'
        self.bet_type = '1x2'
        self.url = 'https://www.betflag.it/exchange'
        self.refresh_period = 600 # seconds
        self.n_driver = 1
        self.offline = offline
        if not self.offline:
            self.live = True
        else:
            self.live = live

        self.headless = headless

        options = Options()
        if self.headless:
            options.headless = True
        self.driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
        if self.offline:
            self.driver.execute_script(f"window.name = 'offline';")
            if self.live:
                self.driver.execute_script(f"window.open('{self.url}', 'live');")
        else:
            self.driver.execute_script(f"window.name = 'live';")

        # Setup the driver
        self.setup_drivers(sport=sport, bet_type=bet_type)
        self.last_refresh = datetime.datetime.now()
        # Login
        time.sleep(1)
        # self.driver.find_element_by_xpath('//*[@id="btnLoginModal"]').click()
        # time.sleep(0.75)
        # self.driver.find_element_by_xpath('//*[@id="LoginUsername"]').send_keys('username')
        # self.driver.find_element_by_xpath('//*[@id="LoginPassword"]').send_keys('password')
        # self.driver.find_element_by_xpath('//*[@id="BtnLoginNew2"]').click()
        if cluster is not None:
            self.client = Client(cluster)
        else:
            self.client = Client(processes=False)

    def setup_drivers(self, sport, bet_type):
        for window in self.driver.window_handles:
            self.driver.switch_to.window(window)
            time.sleep(0.05)
            self.driver.get(self.url)

            # Close +18 warning
            try:
                WebDriverWait(self.driver, timeout=15).until(
                    expected_conditions.element_to_be_clickable(
                        (By.XPATH, '//*[@id="Button1"]')))
                time.sleep(0.5)
                self.driver.find_element_by_xpath('//*[@id="Button1"]').click()
            except TimeoutException:
                pass
            except ElementClickInterceptedException:
                pass

            try:
                WebDriverWait(self.driver, timeout=15).until(
                    expected_conditions.element_to_be_clickable(
                        (By.XPATH, '//*[@id="Button3"]')))
                time.sleep(0.5)
                self.driver.find_element_by_xpath('//*[@id="Button3"]').click()
            except TimeoutException:
                pass
            except ElementClickInterceptedException:
                pass

            # Close promo-login
            try:
                time.sleep(2)
                WebDriverWait(self.driver, timeout=25).until(
                    expected_conditions.element_to_be_clickable((By.XPATH, '//*[@id="PromoPopup"]/div[2]/div[2]')))
                self.driver.find_element_by_xpath('//*[@id="PromoPopup"]/div[2]/div[2]').click()
            except TimeoutException:
                pass

        time.sleep(1)
        self.set_live()
        # Set sport
        self.set_sport(sport)
        # Load additional data
        self.load_additional_data()
        # Set bet_type
        self.set_bet_type(bet_type)

    def set_sport(self, sport):
        # Check if actual sport is equal to the new sport
        if self.sport == sport:
            return
        flag = True
        for window in self.driver.window_handles:
            self.driver.switch_to.window(window)
            time.sleep(0.05)
            # Find the sport button then click it
            for el in self.driver.find_elements_by_xpath('//*[@id="MenuScroller"]/ul/li'):
                if el.text.lower() == sport:
                    el.click()
                    flag = False
                    break
            if flag:
                # If it isn't present close the driver and remove it from the drivers list
                print(f'Sport: {sport} is not available!')
                # self.driver.close()
            flag = True
        self.sport = sport

    def set_bet_type(self, bet_type):
        # Check if actual bet_type is equal to the new bet_type
        if self.bet_type == bet_type or self.sport == 'calcio':
            self.bet_type = bet_type
            return

        if self.bet_type != '1x2':
            is_uo = self.bet_type[:2] == 'uo'
            if is_uo:
                n_goal = self.bet_type[-3:]
                for window in self.driver.window_handles:
                    self.driver.switch_to.window(window)
                    time.sleep(0.05)
                    self.driver.find_element_by_xpath(f"//a[contains(text(), 'Under And Over {n_goal}')]"
                                                      ).find_element_by_xpath('..').click()
        else:
            if bet_type  == '12':
                self.bet_type = bet_type

    def set_live(self):
        # Click on last minute on the first driver
        if self.offline:
            self.driver.switch_to.window('offline')
            time.sleep(0.05)
            try:
                WebDriverWait(self.driver, timeout=15).until(
                    expected_conditions.element_to_be_clickable(
                        (By.XPATH, '/html/body/form/section/div[9]/div[3]/nav/div[2]/ul/li[1]/button'))).click()
            except TimeoutException:
                pass

        # Click on live on the second driver
        if self.live:
            self.driver.switch_to.window('live')
            time.sleep(0.05)
            try:
                WebDriverWait(self.driver, timeout=15).until(
                    expected_conditions.element_to_be_clickable(
                        (By.XPATH, '//*[@id="livenowbutton"]/button'))).click()
            except TimeoutException:
                pass

    def load_additional_data(self):
        # Add data
        self.n_additional_data_loaded = []
        time.sleep(2.5)
        for window in self.driver.window_handles:
            self.driver.switch_to.window(window)
            time.sleep(0.05)
            i = 0
            while i < self.max_additional_data:
                try:
                    self.driver.find_element_by_class_name('addEvents').click()
                    time.sleep(0.25)
                    i += 1
                except NoSuchElementException:
                    break
            self.n_additional_data_loaded.append(i)
            # Scroll to the top of the page
            self.driver.execute_script(f"window.scrollTo(0,0)")
        # Wait until they are loaded
        self.scroll_page()

    def scroll_page(self, wait_period=0.75, jump=500, jump_per_additional_content=4):
        for window, n_additional_data_loaded in zip(self.driver.window_handles, self.n_additional_data_loaded):
            self.driver.switch_to.window(window)
            time.sleep(0.05)
            if n_additional_data_loaded > 10:
                _wait_period = wait_period / math.log(n_additional_data_loaded)
            else:
                _wait_period = wait_period
            for i in range((n_additional_data_loaded + 1) * jump_per_additional_content):
                self.driver.execute_script(f"window.scrollTo(0,{int(jump * i)})")
                time.sleep(_wait_period)

    def refresh_pages(self):
        self.setup_drivers(self.sport, self.bet_type)
        self.last_refresh = datetime.datetime.now()

    def get_data(self):
        data = {}
        # Refresh the page every self.refresh_period seconds
        if self.last_refresh + datetime.timedelta(seconds=self.refresh_period) < datetime.datetime.now():
            self.refresh_pages()
        else:
            # Otherwise scroll the page in order to be sure that all the bets are loaded
            # self.scroll_page(0.01, jump=1000, jump_per_additional_content=1)
            pass
        futures = []
        data = {}
        for window in self.driver.window_handles:
            # Extract HTML code
            self.driver.switch_to.window(window)
            time.sleep(0.05)
            content_html = self.driver.find_element_by_class_name('containerEvents').get_attribute('innerHTML')
            soup = BeautifulSoup(content_html, 'html.parser')
            # Scrape the data
            divs = soup.find_all('div', 'row-e')
            futures = [self.client.submit(parse_row, str(div), self.bet_type) for div in divs]
            results = [future.result() for future in futures]
            data.update({key: value for key, value in results if value})

        return data

    def bet(self):
        pass

    def close(self, close_client=True):
        self.driver.quit()
        if close_client:
            self.client.close()

