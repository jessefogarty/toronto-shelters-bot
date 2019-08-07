#!/usr/bin/env python3
import sqlite3
import datetime
from threader import Threader
from TwitterAPI import TwitterAPI
from test import keys
'''
    A Twitter bot that tweets out Toronto Homeless shelter data.
'''


class TwitterBot():
    def __init__(self):
        # TODO: Add gpg decryption for keys (encrypt creds.py (with keys var))
        self.api = TwitterAPI(**keys)
        conn = sqlite3.connect('shelter_occupancy.db')
        self.cur = conn.cursor()
        self._tweets = []



    def get_data(self, date):
        occupancy_data = self.cur.execute("""select occupancy.date, occupancy.available, occupancy.capacity,
                                            occupancy.occupants, shelters.facility from occupancy
                                            join shelters on occupancy.shelter_id = shelters.id
                                            where date = ? and occupancy.available > 0""", (yesterday,)).fetchall()

        shelter_list = []
        for data in occupancy_data:
            shelter = {}
            shelter['name'] = data[4]
            shelter['available'] = data[1]
            shelter['capacity'] = data[2]
            shelter['occupants'] = data[3]
            shelter_list.append(shelter)

        return shelter_list

    def start_thread(self, data):
        yesterday = datetime.date.fromordinal(datetime.date.today().toordinal()-1).strftime("%F")
        shelters = len(data)
        available = 0  ; occupants = 0
        for each in data:
            available += each['available']
            occupants += each['occupants']
        status = f"Yesterday ({yesterday}) there were a total of {shelters} shelters with {available} beds available in @Toronto. See the thread for more details on each shelter. Data via @Open_TO #onpoli #cdnpoli"
        self._tweets.append(status)

    def thread_add_shelter(self, data):
        for each in data:
            status = f"{each['name']} had {each['available']} bed(s) available yesterday. #endhomelessness #onpoli #toronto"
            self._tweets.append(status)

    def send_tweet(self):
        thread = Threader(self._tweets, self.api, wait=2)
        thread.send_tweets()

if __name__ == "__main__":
    bot = TwitterBot()
    # get yesterday date
    yesterday = datetime.date.fromordinal(datetime.date.today().toordinal()-1).strftime("%F")
    data = bot.get_data(yesterday)
    bot.start_thread(data)
    bot.thread_add_shelter(data)
    bot.send_tweet()
