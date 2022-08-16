from collections import namedtuple
from utility.string_utility import is_equal_club_name

MatchTuple = namedtuple('MatchTuple', ['club1', 'club2', 'date', 'sport'])
match_tuple_size = len(MatchTuple._fields)

BetPrice = namedtuple('BetPrice', ['back_price', 'back_size', 'lay_price', 'lay_size'])
bet_price_size = len(BetPrice._fields)

BetInfo = namedtuple('BetInfo', ['club1', 'club2', 'date', 'sport',
                                 'site', 'bet_type',
                                 'back_price', 'back_size', 'lay_price', 'lay_size'])
bet_info_size = len(BetInfo._fields)

BetComparison = namedtuple('BetComparison', ['club1', 'club2', 'date', 'sport', 'bet_type',
                                             'site1', 'back_price', 'back_size',
                                             'site2', 'lay_price', 'lay_size'])
bet_comparison_size = len(BetComparison._fields)


def is_equal_match(m1, m2):
    return is_equal_club_name(m1[0], m2[0]) and is_equal_club_name(m1[1], m2[1])
