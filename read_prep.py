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
	trips = pd.read_csv(url_trips)

	#Modify Date & Time columns
	trips_df.start_date = pd.to_datetime(trips_df.start_date)
	trips_df.end_date = pd.to_datetime(trips_df.end_date)
	trips=  trips_df.loc[trips_df.start_date.map(lambda t: t.year) == 2012]
	trips['st_hour'] = trips_df.start_date.map(lambda t: t.hour)
	trips['end_hour'] = trips_df.end_date.map(lambda t: t.hour)
	trips['st_minute'] = trips_df.start_date.map(lambda t: t.minute)
	trips['end_minute'] = trips_df.end_date.map(lambda t: t.minute)
	trips_df.st_minute= trips_df.st_minute+60*trips_df.st_hour
	trips_df.end_minute= trips_df.end_minute+60*trips_df.end_hour
	trips['st_month'] = trips_df.start_date.map(lambda t: t.month)
	trips['end_month'] = trips_df.end_date.map(lambda t: t.month)
	trips['st_daydate'] = trips_df.start_date.map(lambda t: t.date())
	trips['end_daydate'] = trips_df.end_date.map(lambda t: t.date())
	trips['st_weekday'] = trips_df.st_daydate.map(lambda t: t.weekday())
	trips['end_weekday'] = trips_df.end_daydate.map(lambda t: t.weekday())
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