import time
import pandas as pd
from scrapers.betflag import BetflagScraper
from scrapers.betfair import BetfairScraper
from single_bet_type_analyzer import SingleBetTypeAnalyzer
from dask.distributed import LocalCluster
from ensemble_bet_analyzer import EnsembleBetAnalyzer
from selenium.common.exceptions import TimeoutException


def print_data(_d):
    print(f'{len(_d)} match')
    for x in _d:
        print(x)
        for y in _d[x]:
            print(y, ':', _d[x][y])


if __name__ == '__main__':
    # cluster = None
    cluster = LocalCluster(processes=False)
    analyzer = EnsembleBetAnalyzer(headless=False)
    # analyzer = SingleBetTypeAnalyzer('tennis', '12', cluster=cluster, headless=False)
    # scraper = BetflagScraper('tennis', '12', cluster=cluster, headless=False)
    # scraper = BetfairScraper(cluster=cluster, headless=False)
    df = pd.DataFrame()
    try:
        for i in range(10):
            # print(i)
            df = analyzer.analyze_bets()
            print(df)
            print('Sleep')
            time.sleep(5)
        # df = scraper.get_data()
    except TimeoutException:
        analyzer.close()
        # scraper.close()
    finally:
        analyzer.close()
        # scraper.close()
    print(df)
