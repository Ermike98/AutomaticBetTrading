from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
import datetime
import pandas as pd
import time
from collections import defaultdict
from utility.date_utility import next_date_given_dayofweek
from scrapers.site_scraper import SiteScraper
from utility.bet_utility import BetPrice
from math import ceil
from dask.distributed import Client
from selenium.common.exceptions import NoSuchElementException
from utility.string_utility import clean_club_name


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


def parse_row(row, bet_type):
    if bet_type == '1x2':
        bet_type_bets_parser = parse_1x2_button
    elif bet_type == '12':
        bet_type_bets_parser = parse_12_button
    elif bet_type.startswith('uo'):
        bet_type_bets_parser = parse_uox_button(bet_type)
    else:
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
        else:
            _hour, _min = map(int, date_string[-1].split(':'))
            if len(date_string) == 2:
                if date_string[0] == 'oggi':
                    date = datetime.date.today()
                else:
                    date = next_date_given_dayofweek(date_string[0])
            else:
                date = datetime.date.today()
                # date = pd.to_datetime(' '.join(date_string[:-1]) + ' ' + str(datetime.date.today().year)).date()

            matchDate = str(date)

    # Find the clubs
    clubs = row.find('ul', class_='runners').find_all('li')
    if clubs:
        clubs_name = [clean_club_name(clubs[0].text),
                      clean_club_name(clubs[1].text)]
        clubs_name.sort()
        сlub1, сlub2 = clubs_name

        # Find bets price and size
        return (сlub1, сlub2, matchDate), bet_type_bets_parser(row)

def parse_content(content_html, bet_type):
    soup = BeautifulSoup(content_html, 'html.parser')
    # Parse the content
    rows = soup.find_all('tr', attrs={"ng-repeat-start": "(marketId, event) in vm.tableData.events"})
    data = {}
    for row in rows:
        key, value = parse_row(row, bet_type=bet_type)
        if value:
            data[key] = value

    return data

def sport_url(sport, only_live=False):
    sports_path = {'calcio': 'calcio-scommesse-1/',
                   'tennis': 'tennis-scommesse-2/',
                   'basket': 'basket-scommesse-7522/'}
    if only_live:
        return 'https://www.betfair.it/exchange/plus/it/' + sports_path[sport] + 'inplay/'
    return 'https://www.betfair.it/exchange/plus/it/' + sports_path[sport]

# driver.switch_to.window(driver.window_handles[n_tab]) | driver.switch_to.window(f'{id}')
# driver.execute_script(f"window.open('{url}', '{id}');")
# driver.quit() chiude tutte le finestre contemporaneamente

class BetfairScraper(SiteScraper):
    def __init__(self, sport='calcio', bet_type='1x2', max_additional_data=-1, cluster=None,
                 offline=True, live=True, headless=True):
        self.sport = sport
        if sport == 'calcio':
            self.bet_type = '1x2'
        else:
            self.bet_type = '12'
        self.offline = offline
        if not self.offline:
            self.live = True
        else:
            self.live = live
        self.url = sport_url(self.sport, only_live=not self.offline)

        self.refresh_period = 600 # seconds
        # Create a dask client
        if cluster is not None:
            self.client = Client(cluster)
        else:
            self.client = Client(processes=False)
        self.headless = headless
        self.driver = None
        # Calculate the number of pages
        self.reset_drivers(1)
        self.n_pages = self.number_of_pages()
        if max_additional_data == -1:
            self.max_pages = self.n_pages
        else:
            self.max_pages = max_additional_data
        # Create the drivers
        self.reset_drivers(min(self.max_pages, self.n_pages))

    def reset_drivers(self, n_drivers=-1):
        if n_drivers == -1:
            if self.driver is not None:
                n_drivers = len(self.driver.window_handles)
            else:
                n_drivers = 1
        if self.driver is not None:
            self.close(close_client=False)

        options = Options()
        if self.headless:
            options.headless = True
        self.driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
        for i in range(n_drivers - 1):
            self.driver.execute_script(f"window.open();")
            time.sleep(0.1)

        self.setup_drivers(self.sport, self.bet_type)

    def setup_drivers(self, sport, bet_type):
        if sport != self.sport:
            # If sport is different from previous sport
            # Set sport
            self.set_sport(sport)
            # Set bet type
            self.set_bet_type(bet_type)
            # Then return
            return

        # Otherwise get the pages
        for i in range(len(self.driver.window_handles)):
            self.driver.switch_to.window(self.driver.window_handles[i])
            time.sleep(0.05)
            self.driver.get(self.url + str(i+1))

        self.last_refresh = datetime.datetime.now()
        # Accept the cookies

        for window in self.driver.window_handles:
            self.driver.switch_to.window(window)
            time.sleep(0.05)
            try:
                WebDriverWait(self.driver, timeout=10).until(
                    expected_conditions.element_to_be_clickable(
                        (By.XPATH, '//*[@id="onetrust-accept-btn-handler"]'))).click()  # Cookies
            except TimeoutException:
                pass

        # Then set the bet type
        self.set_bet_type(bet_type)

    def set_sport(self, sport):
        if sport == self.sport:
            return
        # If sport is different from previous sport
        # Set new sport and url
        self.sport = sport
        # Set the default bet type
        if sport == 'calcio':
            self.bet_type = '1x2'
        else:
            self.bet_type = '12'
        self.url = sport_url(self.sport)
        # Calculate the number of pages
        self.n_pages = self.number_of_pages()
        # Create the drivers
        self.reset_drivers(min(self.max_pages, self.n_pages))

    def set_bet_type(self, bet_type):
        if bet_type == self.bet_type:
            return
        # If bet type is different from previous bet type
        self.bet_type = bet_type
        # If bet type is under/over
        bet_text = ''
        if self.bet_type[:2] == 'uo':
            n_goal = self.bet_type[-3:]
            bet_text = f'Under/Over {n_goal} Goal'
        elif self.bet_type == '1x2':
            bet_text = 'Quote mercato'

        for window in self.driver.window_handles:
            self.driver.switch_to.window(window)
            time.sleep(0.05)
            self.driver.find_elements_by_class_name('selected-option')[-1].click()
            time.sleep(0.1)
            self.driver.find_element_by_xpath(f"//span[contains(text(), '{bet_text}')]").click()

    def number_of_pages(self):
        self.driver.get(self.url)
        time.sleep(1)
        try:
            return len(self.driver.find_element_by_class_name("coupon-page-navigation__bullets"
                                                              ).find_elements_by_tag_name('li'))
        except NoSuchElementException:
            # Number of pages not found
            return 1

    def close(self, close_client=True):
        self.driver.quit()

        if close_client:
            self.client.close()

    def refresh_pages(self, loading_period=2):
        n_pages = self.number_of_pages()
        if self.n_pages == n_pages:
            for window in self.driver.window_handles:
                self.driver.switch_to.window(window)
                time.sleep(0.05)
                self.driver.refresh()
            time.sleep(loading_period)
        else:
            self.n_pages = n_pages
            self.reset_drivers(min(self.max_pages, self.n_pages))
        self.last_refresh = datetime.datetime.now()

    def get_data(self):
        data = {}
        if self.last_refresh + datetime.timedelta(seconds=self.refresh_period) < datetime.datetime.now():
            self.refresh_pages()
        # Parse the content using dask futures
        futures = []
        for window in self.driver.window_handles:
            self.driver.switch_to.window(window)
            time.sleep(0.05)
            futures.append(self.client.submit(parse_content,  # function
                                              self.driver.find_element_by_tag_name('bf-super-coupon'
                                                                                   ).get_attribute('innerHTML'), # content_html
                                              self.bet_type,  # bet_type
                           ))

        for f in futures:
            data.update(f.result())
        return data