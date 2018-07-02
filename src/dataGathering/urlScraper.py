import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
import datetime as dt
import os
import time
import numpy as np
#from gw_utility.logging import Logging

def urlScrape(row,outputFolder):
    id = row[1][0]
    url = row[1][1]
    page = requests.get(url)
    soup = BeautifulSoup(page.content,'html.parser')
    body = soup.find({'article':'id=story'})
    pars = body.find_all('p')
    txt = ''
    for i in pars:
        txt += '\n' + ''.join(i.findAll(text = True))
    if not os.path.exists(outputFolder):
        os.makedirs(outputFolder)
    fname = outputFolder + '/' + id + '.txt'
    with open(fname,'w') as f:
        print(txt,file = f)
#------------------------------------------------------------------------------------------------
# Set file name to read from and range of dates to read:

for topic in ["science"]:
    s = time.time()
    filename = "../../data/raw/" + topic + ".csv"
    outputFolder = "../../data/raw/" + topic

    # Read in url DataFrame
    df = pd.read_csv(filename)
    df = df.drop(df.columns[0], axis = 1)
#    df = df[250:]
#    df.reset_index(drop = True)

    # Scrape each url
    err = []
    for row in df.iterrows():
        print('Row:' + str(row[0]) + '/' + str(len(df)) + '...')
        try:
            urlScrape(row,outputFolder)
            print('done')
            time.sleep(1)
        except AttributeError as error:
            err.append(row[0])
    print('Bad Rows: '+ str(err))
    e = time.time()
    time = np.round(e - s,3)
    print('done in ' + str(time))
