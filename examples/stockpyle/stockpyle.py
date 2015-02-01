import datetime, time
from collections import OrderedDict

import urllib2 
from bs4 import BeautifulSoup

from datapyle import Pyle,Pyler
from datapyle.sqlaimports import *

stockpyler = Pyler("stockpyle.db")

def create_url_get_query (urlbase,**params):
    """
    Parameters
    urlbase : string, must end in ?
        e.g. 'http://download.finance.yahoo.com/d/quotes.csv?'
    **params : for the query 

    """
    url = urlbase
    urlparams = []
    for key in params:
        # TODO: fix values,key for spaces and special characters
        value = params[key].replace(" ","+")
        key = key.replace(" ","+")
        urlparams.append("{}={}".format(key,value))
    url += "&".join(urlparams)
    return url 

def real_time_stock_price (stock_id="GOOG"):
    urlbase = "http://download.finance.yahoo.com/d/quotes.csv?"
    
    params = OrderedDict(\
        f="snl1",
        s=stock_id.upper(),
        )
    
    url = create_url_get_query(urlbase,**params)
    response = urllib2.urlopen(url)
    # TODO: check that url succeeded
    html = response.read()
    stock_id,name,price = html.rstrip().split(",")
    return float(price)

class Stock(object):
    price = Column(Float)
    
    #class attributes
    symbol = None
    
    def __init__(self, price):
        self.price = price
    
    @classmethod
    def fetch(cls, session):
        price = real_time_stock_price(cls.symbol)
        return cls(price)

class Google(Stock, Pyle, stockpyler.PyleBase):
    symbol ="GOOG"
    sampling_interval = datetime.timedelta(minutes=5)

class Apple(Stock, Pyle, stockpyler.PyleBase):
    symbol ="AAPL"
    sampling_interval = datetime.timedelta(minutes=3)

class Nokia(Stock, Pyle, stockpyler.PyleBase):
    symbol ="NOK"
    sampling_interval = datetime.timedelta(minutes=10)


#class TimeOfDay(Pyle, stockpyler.PyleBase, Stock):
#    value = Column(Float)
#    sampling_interval = datetime.timedelta(seconds=1)
#    
#    def __init__(self, value):
#        self.value = value
#    
#    @staticmethod
#    def fetch_current():
#        return TimeOfDay(time.time())

if __name__ == "__main__":
    stockpyler.higher_and_deeper()
    
