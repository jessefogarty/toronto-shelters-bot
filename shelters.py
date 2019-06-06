#!/usr/bin/env python3
'''
    Python3 to download and store shelter occupancy data from City of Toronto.
'''

import sqlite3
import requests
from termcolor import cprint
from tqdm import tqdm
import time
import json

def update_shelters():
    conn = sqlite3.connect('shelter_occupancy.db')
    cur = conn.cursor()
    url = 'https://secure.toronto.ca/c3api_data/v2/DataAccess.svc/ssha/extractssha?$format=application/json;odata.metadata=none&unwrap=true&$top=100000&$select=OCCUPANCY_DATE,ORGANIZATION_NAME,SHELTER_NAME,SHELTER_ADDRESS,SHELTER_CITY,SHELTER_PROVINCE,SHELTER_POSTAL_CODE,FACILITY_NAME,PROGRAM_NAME,SECTOR,OCCUPANCY,CAPACITY&$orderby=OCCUPANCY_DATE,ORGANIZATION_NAME,SHELTER_NAME,FACILITY_NAME,PROGRAM_NAME'
    req = requests.get(url)
    data = req.json()
    shelter_submit = []
    for shelter in data:
        name = shelter['FACILITY_NAME']
        org = shelter['SHELTER_NAME']
        sector = shelter['SECTOR']
        location = shelter['SHELTER_ADDRESS']
        postcode = shelter['SHELTER_POSTAL_CODE']
        city = shelter['SHELTER_CITY']
        beds_max = shelter['CAPACITY']
        shelter_meta = (name, org, sector, location, postcode, city, beds_max)
        shelter_submit.append(shelter_meta)
    cur.executemany('''insert or ignore into shelters
                    (facility, organization, sector, address, postcode, city, capacity)
                    values (?,?,?,?,?,?,?)''', shelter_submit)
    conn.commit()
    conn.close()

def import_occupancy(fname):
    '''
        Import a json file from the city of toronto & store in the database.
            fname : str
    '''
    update_shelters(fname)
    try:
        conn = sqlite3.connect('shelter_occupancy.db')
        cur = conn.cursor()
        submit = []
        f = open(fname)
        data = json.load(f)
        cprint('WORKING: Parsing, cleaning and storing data in shelter_occupancy.db', 'yellow')
        for shelter in tqdm(data):
            name = shelter['FACILITY_NAME']
            shelter_id = cur.execute('''select id from shelters where facility =?''',
                                     (name,)).fetchone()[0]
            beds_max = shelter['CAPACITY']
            if beds_max == None:
                beds_max = 0
            beds_used = shelter['OCCUPANCY']
            if beds_used == None:
                beds_used = 0
            beds_avail = int(beds_max) - int(beds_used)
            date = shelter['OCCUPANCY_DATE']
            occupancy_meta = (date, beds_avail, beds_max, beds_used, shelter_id)
            submit.append(occupancy_meta)

        cprint('WORKING: Storing data in shelter_occupancy.db', 'yellow')
        cur.executemany('''insert or ignore into occupancy values (?,?,?,?,?)''',
                                submit)
        conn.commit()
        conn.close()
        cprint(f'SUCCESS: Finished storing data from {fname}', 'green')
    except FileNotFoundError:
        cprint(f'ERROR: File {fname} not found. Exiting', 'red')

def yesterday_occupancy():
    st = time.time()
    cprint(f'WORKING: Connected to database shelter_occupancy.db.', 'yellow')
    conn = sqlite3.connect('shelter_occupancy.db')
    cur = conn.cursor()
    url = 'https://secure.toronto.ca/c3api_data/v2/DataAccess.svc/ssha/extractssha?$format=application/json;odata.metadata=none&unwrap=true&$top=100000&$select=OCCUPANCY_DATE,ORGANIZATION_NAME,SHELTER_NAME,SHELTER_ADDRESS,SHELTER_CITY,SHELTER_PROVINCE,SHELTER_POSTAL_CODE,FACILITY_NAME,PROGRAM_NAME,SECTOR,OCCUPANCY,CAPACITY&$orderby=OCCUPANCY_DATE,ORGANIZATION_NAME,SHELTER_NAME,FACILITY_NAME,PROGRAM_NAME'
    cprint(f'WORKING: Retreiving data from toronto.ca', 'yellow')
    req = requests.get(url)
    data = req.json()
    cprint(f'SUCCESS: Data retreived!', 'green')
    submit = []
    cprint(f'WORKING: Parsing and cleaning new data', 'yellow')
    for shelter in tqdm(data):
        name = shelter['FACILITY_NAME']
        shelter_id = cur.execute('''select id from shelters where facility =?''',
                                 (name,)).fetchone()[0]
        try:
            beds_max = shelter['CAPACITY']
            beds_used = shelter['OCCUPANCY']
            beds_avail = int(beds_max) - int(beds_used)
        except TypeError:
            beds_avail = 0
        date = shelter['OCCUPANCY_DATE']
        temp = None
        occupancy_meta = (date, beds_avail, beds_max, beds_used, shelter_id, temp)
        submit.append(occupancy_meta)

    cprint('WORKING: Storing data in shelter_occupancy.db', 'yellow')
    cur.executemany('''insert or ignore into occupancy values (?,?,?,?,?,?)''',
                            submit)
    conn.commit()
    conn.close()
    ft = time.time() - st
    cprint(f'SUCCESS: Finished parsing and storing data in {ft} seconds.', 'green')

def update_weather():
    api = '376f33547bb1fb9edf121ac6c33603e8'
    lat = '43.6529'
    lng = '-79.3849'

    conn = sqlite3.connect('shelter_occupancy.db')
    cur = conn.cursor()

    date_temp = {}

    _dates = cur.execute('select distinct(date) from occupancy where temperature is null').fetchall()

    for each in _dates:
        date_temp[each[0]] = None

    for k, v in date_temp.items():
        dtime = '12:00:00'
        date = k
        temp = v
        url = f'https://api.darksky.net/forecast/{api}/{lat},{lng},{date}T{dtime}?units=si&exclude=daily,minutely,hourly'
        req = requests.get(url)
        data = req.json()
        # Catch no temp for date error - ie aug 28, 2018 no darksky data.
        try:
            temp = data['currently']['temperature']
            date_temp[date] = data['currently']['temperature']
        except KeyError as key_ex:
            print(key_ex.args)
            date_temp[date] = None


    for k, v in date_temp.items():
        date = k
        temp = v
        cur.execute('update occupancy set temperature = ? where date = ?', (temp, date))

    conn.commit()
    conn.close()

if __name__ == "__main__":
    update_shelters()
    yesterday_occupancy()
    update_weather()
