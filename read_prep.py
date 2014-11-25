import pandas as pd # pandas
import datetime
from StringIO import StringIO
import requests

#####--------------- TRIPS -------------------#####

"""
Function
--------
trips
Import the Hubway trips dataframe from specified url and prepare data by added adequate columns (Date/Time Columns)

Parameters
----------
url_trips : str
	url of csv trips file to be imported

Returns
-------
trips : pandas.dataframe
    Pandas dataframe containing all trip data for 2012

Example
-------
>>> read_prep_trips('https://raw.githubusercontent.com/CS109Hubway/classp/master/data/tripsthrough2012.csv')
"""
def trips(url_trips):
	trips_df = pd.read_csv(url_trips)

	#Modify Date & Time columns
	trips_df.start_date = pd.to_datetime(trips_df.start_date)
	trips_df.end_date = pd.to_datetime(trips_df.end_date)
	trips_df=  trips_df.loc[trips_df.start_date.map(lambda t: t.year) == 2012]
	trips_df['st_hour'] = trips_df.start_date.map(lambda t: t.hour)
	trips_df['end_hour'] = trips_df.end_date.map(lambda t: t.hour)
	trips_df['st_minute'] = trips_df.start_date.map(lambda t: t.minute)
	trips_df['end_minute'] = trips_df.end_date.map(lambda t: t.minute)
	trips_df.st_minute= trips_df.st_minute+60*trips_df.st_hour
	trips_df.end_minute= trips_df.end_minute+60*trips_df.end_hour
	trips_df['st_month'] = trips_df.start_date.map(lambda t: t.month)
	trips_df['end_month'] = trips_df.end_date.map(lambda t: t.month)
	trips_df['st_daydate'] = trips_df.start_date.map(lambda t: t.date())
	trips_df['end_daydate'] = trips_df.end_date.map(lambda t: t.date())
	trips_df['st_weekday'] = trips_df.st_daydate.map(lambda t: t.weekday())
	trips_df['end_weekday'] = trips_df.end_daydate.map(lambda t: t.weekday())
	trips_df.reset_index(inplace=True)
	
	return trips_df

#####--------------- WEATHER -------------------#####
"""
Function
--------
weather
Import the previously scrapped hourly weather dataset

Parameters
----------
url_weather : str
	url of weather csv file to be imported

Returns
-------
weather : panda.dataframe
    Pandas dataframe containing all weather data in the file specified by url_weather

Example
-------
>>> read_prep_trips('https://raw.githubusercontent.com/CS109Hubway/classp/master/data/weather.csv')
"""

def weather(url):
	weather_df = pd.read_csv(StringIO(requests.get(url).content), index_col=0)
	weather_df.reset_index(inplace=True)
	weather_df.drop('timestamp', axis=1, inplace=True)
	weather_df['datetime'] = pd.to_datetime(weather_df.datetime)
	weather_df['hour'] = weather_df.datetime.map(lambda t: t.hour)
	weather_df['minute'] = weather_df.datetime.map(lambda t: t.minute)
	weather_df['month'] = weather_df.datetime.map(lambda t: t.month)
	weather_df['daydate'] = weather_df.datetime.map(lambda t: t.date())
	weather_df['weekday'] = weather_df.datetime.map(lambda t: t.weekday())
	
	return weather_df
	

#####--------------- STATION STATUS -------------------#####

"""
Function
--------
status
	Returns a dataframe with Hubway station status data for 2012 from 3 separate input csv files. 

Parameters
----------
url1 : string
    url of first csv file
url2 : string
    url of second csv file
url3 : string
    url of third csv file

Returns
-------
a DataFrame
	A pandas DataFrame containing station status data for 2012. Station status entries with capacity=0 are removed.

Example
-------
>>> status('https://raw.githubusercontent.com/CS109Hubway/classp/master/data/stationstatus8_11to2_12_5min.csv',
	'https://raw.githubusercontent.com/CS109Hubway/classp/master/data/stationstatus3_12to6_12_5min.csv',
	'https://raw.githubusercontent.com/CS109Hubway/classp/master/data/stationstatus7_12to10_12_5min.csv')
"""

def status(url1,url2,url3):
	#import CSVs
	status1 = pd.read_csv(url1, header=None)
	status2 = pd.read_csv(url2, header=None)
	status3 = pd.read_csv(url3, header=None)
	
	#combine station status dataframes
	status_df = pd.concat([status1,status2,status3])
	status_df.columns = ['uniqueid', 'stationid', 'timestamp', 'nbBikes','nbEmptyDocks', 'capacity']
	
	#extract time and date fields from timestamp
	status_df.timestamp = pd.to_datetime(status_df.timestamp)
	status_df = status_df.loc[status_df.timestamp.map(lambda t: t.year) == 2012]
	status_df = status_df.loc[status_df.capacity != 0]
	status_df['hour'] =  status_df.timestamp.map(lambda t: t.hour)
	status_df['minute'] = status_df.timestamp.map(lambda t: t.minute)
	status_df.minute = status_df.minute+60*status_df.hour
	status_df['month'] =  status_df.timestamp.map(lambda t: t.month)
	status_df['daydate'] = status_df.timestamp.map(lambda t: t.date())
	status_df['weekday'] =  status_df.timestamp.map(lambda t: t.weekday())
	status_df.reset_index(inplace=True)

	return status_df
	


def statusintervals(url1,url2):
	#import CSVs
	status1 = pd.read_csv(url1)
	status2 = pd.read_csv(url2)
	
	#combine station status dataframes
	status_df = pd.concat([status1,status2])
	
	#extract time and date fields from timestamp
	status_df.interv = pd.to_datetime(status_df.interv)
	status_df = status_df.loc[status_df.interv.map(lambda t: t.year) == 2012]
	status_df = status_df.loc[status_df.latest_capacity != 0]
	status_df['hour'] =  status_df.interv.map(lambda t: t.hour)
	status_df['minute'] = status_df.interv.map(lambda t: t.minute)
	status_df.minute = status_df.minute+60*status_df.hour
	status_df['month'] =  status_df.interv.map(lambda t: t.month)
	status_df['daydate'] = status_df.interv.map(lambda t: t.date())
	status_df['weekday'] =  status_df.interv.map(lambda t: t.weekday())
	status_df.reset_index(inplace=True)

	return status_df

	
#####--------------- STATIONS -------------------#####

"""
Function
--------
stations
	Returns a dataframe with Hubway station data. 

Parameters
----------
url : string
    url of input csv file


Returns
-------
a DataFrame
	A pandas DataFrame containing station data.

Example
-------
>>> stations('https://raw.githubusercontent.com/CS109Hubway/classp/master/data/hubway_stations.csv')
"""

def stations(url):

	#read in station data
	stations_df = pd.read_csv(url)	
	
	return stations_df


#####--------------- BASEBALL -------------------#####
	
"""
Function
--------
baseball
	Returns a dataframe with Red Sox home games for 2012. 

Parameters
----------
url : string
    url of input csv file


Returns
-------
a DataFrame
	A pandas DataFrame containing Red Sox game times and durations.

Example
-------
>>> baseball('https://raw.githubusercontent.com/CS109Hubway/classp/master/data/RedSox2012.csv')
"""

def baseball(url):
	#read in baseball data
	MLB = pd.read_csv(url, ', ')

	#use date, year, time, and duration fields to generate start and end time stamps and drop temp fields
	MLB['start_time']=pd.to_datetime(MLB['Date']+' '+MLB['Year'].astype(str)+ ' ' +MLB['Time'])
	MLB['Durhour']=MLB['Duration'].apply(lambda x: x.split(':')[0]).astype(int)
	MLB['Durmin']=MLB['Duration'].apply(lambda x: x.split(':')[1]).astype(int)
	MLB['timedelta']=MLB['Durhour'].apply(lambda x: datetime.timedelta(hours=x))+MLB['Durmin'].apply(lambda x: datetime.timedelta(minutes=x))
	MLB['end_time'] = MLB["start_time"]+MLB["timedelta"]
	MLB['hour'] = MLB.start_time.map(lambda t: t.hour)
	MLB['minute'] = MLB.start_time.map(lambda t: t.minute)
	MLB['month'] = MLB.start_time.map(lambda t: t.month)
	MLB['daydate'] = MLB.start_time.map(lambda t: t.date())
	MLB['weekday'] = MLB.start_time.map(lambda t: t.weekday())
	MLB.drop(['Durhour', 'Durmin', 'timedelta'], axis=1, inplace=True)

	return MLB
	
	#####--------------- BASEBALL -------------------#####
	
"""
Function
--------
basketball
	Returns a dataframe with Celtics home games between May and Sept 2012. 

Parameters
----------
url : string
    url of input csv file


Returns
-------
a DataFrame
	A pandas DataFrame containing Celtic game times and durations.

Example
-------
>>> basketball('https://raw.githubusercontent.com/CS109Hubway/classp/master/data/Celtics2012.csv')
"""

def basketball(url):
	#read in baseball data
	NBA = pd.read_csv(url, ', ')

	#use date, year, time, and duration fields to generate start and end time stamps and drop temp fields
	NBA['start_time']=pd.to_datetime(NBA['Date']+' '+NBA['Year'].astype(str)+ ' ' +NBA['Time'])
	NBA['Durhour']=NBA['Duration'].apply(lambda x: x.split(':')[0]).astype(int)
	NBA['Durmin']=NBA['Duration'].apply(lambda x: x.split(':')[1]).astype(int)
	NBA['timedelta']=NBA['Durhour'].apply(lambda x: datetime.timedelta(hours=x))+NBA['Durmin'].apply(lambda x: datetime.timedelta(minutes=x))
	NBA['end_time'] = NBA["start_time"]+NBA["timedelta"]
	NBA['hour'] = NBA.start_time.map(lambda t: t.hour)
	NBA['minute'] = NBA.start_time.map(lambda t: t.minute)
	NBA['month'] = NBA.start_time.map(lambda t: t.month)
	NBA['daydate'] = NBA.start_time.map(lambda t: t.date())
	NBA['weekday'] = NBA.start_time.map(lambda t: t.weekday())
	NBA.drop(['Durhour', 'Durmin', 'timedelta'], axis=1, inplace=True)
	NBA= NBA.loc[NBA.month >=5]
	
	return NBA
