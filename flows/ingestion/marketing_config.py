from enum import Enum


class Frequency(str, Enum):
    """
    https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases
    """

    daily = "D"
    hourly = "h"


channels = [
    "facebook_ads",
    "adwords",
    "bing",
    "criteo",
    "twitter",
    "linkedin",
    "newsletter",
    "google",
    "email",
]
campaigns = [
    "20percentpromocode",
    "15percentpromo",
    "specialday4u",
    "takecare",
    "club42discount",
    "pizzapatrol",
]
