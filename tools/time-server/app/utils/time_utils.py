# -*- coding: utf-8 -*-
import datetime
import faker
__TimeFake = faker.Faker()


def time_utils_random_rfc3339_time(passed_days:int):
    fake_time = __TimeFake.date_time_between(start_date="-{}d".format(passed_days), end_date="now")
    fake_timestamp = datetime.datetime.timestamp(fake_time)
    return datetime.datetime.fromtimestamp(fake_timestamp).isoformat()[:19]


def time_utils_random_timestamp(passed_days:int):
    fake_time = __TimeFake.date_time_between(start_date='-{}d'.format(passed_days), end_date='now')
    return int(datetime.datetime.timestamp(fake_time))
