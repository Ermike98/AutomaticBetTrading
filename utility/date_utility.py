import datetime


def next_date_given_dayofweek(dayofweek, weekdays=None):
    if weekdays is None:
        weekdays = ['lun', 'mar', 'mer', 'gio', 'ven', 'sab', 'dom']
    if dayofweek not in weekdays:
        return None
    for i in range(7):
        date = datetime.datetime.now().date() + datetime.timedelta(days=i)
        if weekdays[date.weekday()] == dayofweek:
            return date
