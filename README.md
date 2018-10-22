# coinmarketcap-to-mongodb
A scraper that fetches the data from coinmarketcap.com/historical/xxxxxxxx and saves it to a mongodb for future use

# usage
Clone repo. Make sure to specify any dates you wish to save in the run_fetcher.py as well as the server details in config.py

# mongo
This script uses a mongodb. You need to set one up and run it, locally or somewhere else, in order for you to save the data. Google is your friend. When up and running, specify address, database title and collection name in config.py
