import numpy as np
import pandas as pd # pandas
import datetime as dt # module for manipulating dates and times
import scipy.stats as stats
import iso8601
import time
import datetime as dt
import requests

"""
Function
--------
scrape_weather 

scrape weather from weathersource API

Parameters
----------
from_date : date
    First day of the range for which weather data should be obtained
	
to_date : date
    Last day of the range for which weather data should be obtained
	
Returns
-------
csv : pandas.dataframe
    Weather dataframe for dates specified

Example
-------
>>> scrape_weather(dt.date(2012,3,10),dt.date(2012,11,30))
"""

def scrape_weather(from_date,to_date):

	d1 = dt.date(2012,3,10)
	d2 = dt.date(2012,11,30)
	delta = to_date - from_date
	dates = [str(d1+dt.timedelta(days=i)) for i in range(delta.days+1)]
	weather = pd.DataFrame(columns=['precip', 'snowfall', 'temp', 'timestamp', 'datetime'])

	# Loop through all days of interest and obtain hourly weather
	for day in dates:
		url = "https://api.weathersource.com/v1/72ca38efeac35b0dfea6/history_by_postal_code.json?period=hour&postal_code_eq=02139&country_eq=US&limit=24&timestamp_between=%sT00:00+00:00,%sT24:00+00:00&fields=timestamp,snowfall,temp,precip"% (day,day)
		response = requests.get(url).json()
		df = pd.DataFrame(response)
		df['datetime'] = [iso8601.parse_date(hour['timestamp']) for hour in response]
		weather = weather.append(df)
		time.sleep(6.1)
		print str(day)
	weather.reset_index(inplace=True)
	weather.drop('index', axis=1, inplace=True)
	
	return weather


weather = scrape_weather(dt.date(2012,3,10),dt.date(2012,11,30))    
weather.to_csv(path_or_buf = 'C:\Users\Gabriel\Documents\CS109\classp\data\weather.csv')   