from typing import List
from database.database_login import username, password, host
from utility.bet_utility import BetInfo, BetPrice
from utility.bet_utility import match_tuple_size as mts, bet_price_size as bps
from collections import defaultdict
import pandas as pd
from utility.bet_utility import is_equal_match
from utility.string_utility import simpler_club_name, is_equal_club_name
from dask.distributed import Client


def find_match_index(matches_df, match):
    for i in matches_df.index[::-1]:
        if is_equal_match(match, tuple(matches_df.loc[i])):
            # Return index
            return i
    return None


class BetContainer:
    def __init__(self, cluster=None):
        # self.conn = connector.connect(host=host,
        #                               user=username,
        #                               password=password,
        #                               database='bets')
        # self.c = self.conn.cursor()

        # Create a dask client
        if cluster is not None:
            self.client = Client(cluster)
        else:
            self.client = Client(processes=False)
        self._df = pd.DataFrame(columns=['match_id'] + list(BetInfo._fields[mts:]))
        self.matches = pd.DataFrame(columns=BetInfo._fields[:mts])
        self._data = defaultdict(lambda: defaultdict(dict))
        self._matches_id = {}
        self._inverse_matches_id = {}
        self._clubs_id = {}
        self._inverse_clubs_id = {}

    # def insert_match_into_database(self, match_tuple):
    #     sql = """INSERT INTO `bets`.`match` (`club1`, `club2`, `date`, `sport`)
    #                  VALUES (%s, %s, %s, %s);"""
    #     self.c.execute(sql, match_tuple)
    #     self.conn.commit()

    # def insert_bets_into_database(self, bet_info: BetInfo):
    #     sql = """INSERT INTO `bets`.`bet`
    #         (`match_id`, `site`, `bet_type`, `back_price`, `back_size`, `lay_price`, `lay_size`)
    #         VALUES (%s, %s, %s, %s, %s, %s, %s);"""
    #     match_id = self.find_match_id(bet_info[:mts])
    #     if match_id is None:
    #         self.insert_match_into_database(bet_info[:mts])
    #         match_id = self.find_match_id(bet_info[:mts])
    #     val = (match_id,) + bet_info[mts:]
    #     self.c.execute(sql, val)
    #     self.conn.commit()

    def get_club_id(self, club_name):
        if club_name in self._clubs_id:
            return self._clubs_id[club_name]
        for _id, _name in self._inverse_clubs_id.items():
            if is_equal_club_name(_name, club_name):
                self._clubs_id[club_name] = _id
                self._inverse_clubs_id[_id] = simpler_club_name(_name, club_name)
                return self._clubs_id[club_name]
        self._clubs_id[club_name] = len(self._clubs_id)
        self._inverse_clubs_id[self._clubs_id[club_name]] = club_name
        return self._clubs_id[club_name]

    def to_match_with_club_names(self, match):
        return self._inverse_clubs_id[match[0]], self._inverse_clubs_id[match[1]], *match[2:]

    def to_match_with_club_ids(self, match):
        return self.get_club_id(match[0]), self.get_club_id(match[1]), *match[2:]

    def update_matches(self, matches):
        # Remove duplicates from matches
        matches = list(dict.fromkeys(matches))
        # Create a list for the matches ids
        ids = []
        # For each match
        for match in matches:
            # Transform club name in club id
            match_tuple = self.to_match_with_club_ids(match)
            # If the match is not in the matches dictionary
            if match_tuple not in self._matches_id:
                # Add it into them
                self._matches_id[match_tuple] = len(self._matches_id)
                self._inverse_matches_id[self._matches_id[match_tuple]] = match_tuple
            # Then append the its id in ids
            ids.append(self._matches_id[match_tuple])
        # Return the dictionary [match]->id => self._matches_id on matches given in input
        return dict(zip(matches, ids))

    def update_bets(self, bets_list: List[BetInfo]):
        # Remove empty bets
        # print('\t\tFilter the bets')
        bets_list = list(filter(lambda bet: bet.back_price != 0 and bet.lay_price != 0 and bet.club1 and bet.club2,
                                bets_list))
        # print('\t\tUpdate matches')
        matches_id = self.update_matches([bet[:mts] for bet in bets_list])
        # Transform bets list
        # print('\t\tAppend bets')
        for bet_info in bets_list:
            match_id = matches_id[bet_info[:mts]]
            self._data[bet_info.bet_type][match_id][bet_info.site] = bet_info[-bps:]

    def search_bets_by_bet_type(self, bet_type: str, return_only_multiple_bets=True):
        matches_bets = dict(
            filter(lambda x: len(x[1]) > 1 and return_only_multiple_bets or not return_only_multiple_bets,
                   self._data[bet_type].items()
                   )
        )
        result = {}
        for match_id, match_bets in matches_bets.items():
            # For each match create a DataFrame containing its bets
            match_bets_dict = defaultdict(list)
            for site, bet_price in match_bets.items():
                match_bets_dict['site'].append(site)
                for bet_price_field, bet_price_entry in zip(BetPrice._fields, bet_price):
                    match_bets_dict[bet_price_field].append(bet_price_entry)
            result[self.to_match_with_club_names(self._inverse_matches_id[match_id])] = pd.DataFrame(match_bets_dict)

        return result

    def bet_types(self):
        return self._data.keys()

    @property
    def df(self):
        return self._df
