from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By
import datetime
import pandas as pd
import time
from collections import defaultdict
from utility.date_utility import next_date_given_dayofweek
from scrapers.site_scraper import SiteScraper
from utility.bet_utility import BetPrice
from math import ceil
from dask.distributed import Client
import selenium.common

drivers = {}


def parse_1x2_button(row):
    # Find bet price and size
    bet_buttons = row.find('td', class_='coupon-runners').find_all('span')
    bets = [button.text if button.text[0] != '€' else button.text[1:] for button in bet_buttons]
    return {'1': BetPrice(*map(float, bets[:4])),
            'x': BetPrice(*map(float, bets[4:8])),
            '2': BetPrice(*map(float, bets[8:12]))}


def parse_12_button(row):
    # Find bet price and size
    bet_buttons = row.find('td', class_='coupon-runners').find_all('span')
    bets = [button.text if button.text[0] != '€' else button.text[1:] for button in bet_buttons]
    return {'1': BetPrice(*map(float, bets[:4])),
            '2': BetPrice(*map(float, bets[4:8]))}


def parse_uo_button(row):
    # Find bet price and size
    bet_buttons = row.find('td', class_='coupon-runners').find_all('span')
    bets = [button.text if button.text[0] != '€' else button.text[1:] for button in bet_buttons]
    return {'u': BetPrice(*map(float, bets[:4])),
            'o': BetPrice(*map(float, bets[4:8]))}


def parse_uo1_button(row):
    bets = parse_uo_button(row)
    return {'u1.5': bets['u'], 'o1.5': bets['o']}


def parse_uo2_button(row):
    bets = parse_uo_button(row)
    return {'u2.5': bets['u'], 'o2.5': bets['o']}


def parse_uo3_button(row):
    bets = parse_uo_button(row)
    return {'u3.5': bets['u'], 'o3.5': bets['o']}


def parse_uo4_button(row):
    bets = parse_uo_button(row)
    return {'u4.5': bets['u'], 'o4.5': bets['o']}


def parse_row(row, bet_type):
    bet_type_bets_parsers = {'1x2': parse_1x2_button,
                             '12': parse_12_button,
                             'uo1.5': parse_uo1_button,
                             'uo2.5': parse_uo2_button,
                             'uo3.5': parse_uo3_button,
                             'uo4.5': parse_uo4_button}
    if bet_type not in bet_type_bets_parsers:
        raise Exception(f"bet_type {bet_type} is not allowed!")

    # Check if it is live
    start_date_wrapper = row.find('div', class_='start-date-wrapper')
    if start_date_wrapper is None or start_date_wrapper.text.lower() == 'live':
        # ...
        matchDate = str(datetime.datetime.today().date())
    else:
        # Find the date
        date_string = row.find('div', class_='start-date-wrapper').text.lower().split()
        if date_string[0] == 'inizia':
            matchDate = str(datetime.date.today())
            # if date_string[-1] == 'poco':
            #     data['MatchDate'] = str(datetime.datetime.now())
            # else:
            #     date = datetime.date.today()
            #     data['MatchDate'] = str(pd.to_datetime(date) + datetime.timedelta(minutes=int(date_string[-1][:-1])))
        else:
            _hour, _min = map(int, date_string[-1].split(':'))
            if len(date_string) == 2:
                if date_string[0] == 'oggi':
                    date = datetime.date.today()
                else:
                    date = next_date_given_dayofweek(date_string[0])
            else:
                date = pd.to_datetime(' '.join(date_string[:-1]) + ' ' + str(datetime.date.today().year)).date()

            matchDate = str(date)
            # data['MatchDate'] = str(pd.to_datetime(date) + datetime.timedelta(minutes=_min,
            #                                                                   hours=_hour))

    # Find the clubs
    clubs = row.find('ul', class_='runners').find_all('li')
    if clubs:
        club1 = clubs[0].text.lower()
        club2 = clubs[1].text.lower()

        # Find bets price and size
        return (club1, club2, matchDate), bet_type_bets_parsers[bet_type](row)


def parse_content(page, sport, bet_type, driver_index, url, loading_period=2):
    driver = drivers[(sport, bet_type)][driver_index]
    # Load the page required
    driver.get(url + str(page))
    # Wait the page loading
    time.sleep(loading_period)
    # Extract the HTML code
    content_html = driver.find_element_by_tag_name('bf-super-coupon').get_attribute('innerHTML')
    soup = BeautifulSoup(content_html, 'html.parser')
    # Parse the content
    rows = soup.find_all('tr', attrs={"ng-repeat-start": "(marketId, event) in vm.tableData.events"})
    data = {}
    for row in rows:
        key, value = parse_row(row, bet_type=bet_type)
        data[key] = value

    return data


def sport_url(sport):
    sports_path = {'calcio': 'calcio-scommesse-1/',
                   'tennis': 'tennis-scommesse-2/',
                   'basket': 'basket-scommesse-7522/'}
    return 'https://www.betfair.it/exchange/plus/it/' + sports_path[sport]


# driver.find_element_by_xpath("//span[contains(text(), 'Under/Over 1.5 Goal')]").click()
class BetfairScraper(SiteScraper):
    def __init__(self, sport='calcio', bet_type='1x2', max_pages=10, n_drivers=-1, cluster=None):
        self.sport = sport
        self.bet_type = bet_type
        self.driver_id = (sport, bet_type)
        self.max_pages = max_pages
        self.url = sport_url(self.sport)
        # Create the drivers
        # Create the additional drivers
        if n_drivers == -1:
            self.reset_drivers(1)
        else:
            self.reset_drivers(n_drivers)
        # Create a dask client
        if cluster is not None:
            self.client = Client(cluster)
        else:
            self.client = Client(processes=False)
        # Calculae the number of pages
        self.n_pages = self.number_of_pages()
        if n_drivers == -1:
            self.reset_drivers(min(self.max_pages, self.n_pages))

    def setup_drivers(self):
        # Accept cookies
        global drivers
        # Get the pages
        for driver in drivers[self.driver_id]:
            driver.get(self.url)
        # Accept the cookies
        for driver in drivers[self.driver_id]:
            WebDriverWait(driver, timeout=15).until(
                expected_conditions.element_to_be_clickable(
                    (By.XPATH, '//*[@id="onetrust-accept-btn-handler"]'))).click()  # Cookies
        self.set_bet_type()

    def set_bet_type(self):
        global drivers
        if self.bet_type != '1x2':
            is_uo = self.bet_type[:2] == 'uo'
            if is_uo:
                n_goal = self.bet_type[-3:]
            for driver in drivers[self.driver_id]:
                driver.find_elements_by_class_name('selected-option')[-1].click()
                time.sleep(0.1)
                if is_uo:
                    driver.find_element_by_xpath(f"//span[contains(text(), 'Under/Over {n_goal} Goal')]").click()

    def close(self):
        global drivers
        if self.driver_id in drivers and drivers[self.driver_id] is not None:
            for driver in drivers[self.driver_id]:
                driver.close()

    def reset_drivers(self, n_drivers=1):
        global drivers
        self.close()
        drivers[self.driver_id] = [webdriver.Chrome() for i in range(n_drivers)]
        self.setup_drivers()

    def number_of_pages(self):
        global drivers
        drivers[self.driver_id][0].get(self.url)
        time.sleep(1)
        try:
            return len(drivers[self.driver_id][0].find_element_by_class_name(
                "coupon-page-navigation__bullets").find_elements_by_tag_name('li'))
        except selenium.common.exceptions.NoSuchElementException:
            # print('N. pages not found!')
            return 1

    # --- la vita in un giorno ---
    def get_data(self):
        global drivers
        data = {}
        n_pages = min(self.max_pages, self.n_pages)
        n_drivers = len(drivers[self.driver_id])
        # Get and parse the data
        for stage_id in range(ceil(n_pages / n_drivers)):
            # Run n_drivers self.parse_content togheter in order to reduce wasted time
            futures = [self.client.submit(parse_content,  # function
                                          i + n_drivers * stage_id + 1,  # page
                                          self.sport,  # sport
                                          self.bet_type,  # bet_type
                                          i,  # driver index
                                          self.url  # url
                                          ) for i in range(min(n_drivers, n_pages - n_drivers * stage_id))]
            results = [f.result() for f in futures]
            for r in results:
                data.update(r)
        return data

    def bet(self):
        pass
