#!/usr/bin/python
# flake8: noqa: E501
# coding: utf-8

import sys
import configparser
from urllib.request import urlopen
import json
import datetime
import os
import codecs
import pytz
import re

main_base = os.path.dirname(__file__)
config_file = os.path.join(main_base, "config", "prod.cfg")

config = configparser.ConfigParser()
config.read_file(codecs.open(config_file, 'r', 'utf8'))

mqtt_port = config.getint('MQTT', 'port')
mqtt_ip = config.get('MQTT', 'ip')
mqtt_topic_today = config.get('MQTT', 'today_pub')
mqtt_topic_tomorrow = config.get('MQTT', 'tomorrow_pub')
dir_path = config.get('Nordpool', 'cache_dir')
if (len(config.get('MQTT', 'username')) and len(config.get('MQTT', 'password'))):
    auth = {'username': config.get('MQTT', 'username'), 'password': config.get('MQTT', 'password')}
else:
    auth = {}

dt_today = datetime.date.today().strftime("%d-%m-%Y")
orig_tz = pytz.timezone('CET')
local_tz = pytz.timezone(config.get('Timezone', 'timezone'))
normalized_data = {}


def load_json(date):
    data = json.load(open('{}/{}.json'.format(dir_path, date)))
    for row in data['data']['Rows']:
        start_time_notz = datetime.datetime.strptime(row['StartTime'], '%Y-%m-%dT%H:%M:%S')
        rowname = row['Name'].replace('&nbsp;', '')
        if re.search('\d+-\d+', rowname):
            for col in row['Columns']:
                coldate = datetime.datetime.strptime(col['Name'], '%d-%m-%Y')
                col_start_time = start_time_notz.replace(coldate.year, coldate.month, coldate.day)
                col_start_time_origtz = col_start_time.replace(tzinfo=orig_tz)
                start_time = local_tz.normalize(col_start_time_origtz)
                normalized_data[start_time] = col['Value'].replace(',','.').replace(' ','')


#We only have next day prices after 14:00
now = datetime.datetime.now().replace(tzinfo=local_tz)
if now.time() >= datetime.time(14,35):
    dt_tomorrow = (datetime.date.today()+ datetime.timedelta(days=1)).strftime("%d-%m-%Y")
    load_json(dt_tomorrow)
else:
    load_json(dt_today)

now = now.replace(minute=0)

print("<html><body><table>")
for i, d in enumerate(sorted(normalized_data.items())):
    if d[0] >= now:
        a = d[0].strftime('%d/%H')
        b = round(((float(d[1]) / 1000) + 0.064) * 1.21, 3);
        print(f"<tr><td>{a}</td><td>{b}</td></tr>")
print("</table></body></html>")


##Get todays prices
#publish_price(mqtt_topic_today, dt_today)

#if now.time() >= datetime.time(14,35):
#    publish_price(mqtt_topic_tomorrow, dt_tomorrow)
