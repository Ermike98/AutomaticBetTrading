import re


def clean_club_name(club):
    club = club.lower()
    # Remove all the non letter chars from club
    club = re.sub("[^a-z]+", " ", club)
    # Remove words whose length is less than 2
    club_list = list(filter(lambda x: len(x) > 2, club.split()))
    # Sort the words
    club_list.sort()
    # Reassemble the club string
    club = ' '.join(club_list)
    return club


def is_equal_club_name(club1, club2):
    return len(set(club1.split()).intersection(club2.split())) > 0


def simpler_club_name(name1, name2):
    l1 = len(name1)
    l2 = len(name2)
    if l1 < l2:
        return name1
    return name2
