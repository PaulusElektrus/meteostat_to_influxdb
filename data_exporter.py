from influxdb import DataFrameClient
import configparser
from meteostat import Hourly
from datetime import datetime
 
config = configparser.ConfigParser()
config.read_file(open('./token.config', mode='r'))
host = config.get('config', 'host')
user = config.get('config', 'user')
password = config.get('config', 'password')
dbname = config.get('config', 'dbname')

client = DataFrameClient(host, 8086, user, password, dbname)

def get():
    start = datetime(2000, 1, 1)
    end = datetime(2020, 1, 1)

    place = '10866'

    data = Hourly(place, start, end)
    ("Getting Data")
    data = data.fetch()
    return data

def write(data):
    databases = client.get_list_database()
    databaseAlreadyThere = False

    for item in databases:
        if item['name'] == dbname:
            databaseAlreadyThere = True
        else:
            client.create_database(dbname)

    client.switch_database(dbname)
    
    print("Writing Data to Database")
    if client.write_points(data, dbname, protocol = 'line'):
        return
    else:
        print("Daten konnten nicht gesendet werden.")
        
        
if __name__ == '__main__':
    data = get()
    print(data)
    write(data)
    print("YAY")