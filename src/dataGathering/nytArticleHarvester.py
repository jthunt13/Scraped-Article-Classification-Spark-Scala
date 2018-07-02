import pandas as pd;
import requests;
import getpass;
import datetime
import pymysql
import warnings
import json
import time
import math
import os
from sqlalchemy import create_engine

# home made function to store authentications
import loginInfo;

# Ignore warnings
warnings.filterwarnings("ignore", category = Warning)

# variables to change
searchTerms = ["Politics","Sports","Business","Science"]
searchTable = ["nytPoliticsDocs","nytSportsDocs","nytBusinessDocs","nytScienceDocs"]

direction = "newest"
pagesToScanThrough = 200
#------------------------------------------------------------------------------
#                           NYT api Function
#------------------------------------------------------------------------------
def getArticleURL(searchTerm, pages, direction,key):
    df = pd.DataFrame(columns = ["docID","docURL","docDate"])
    for i in range(0,pages):
        print("Getting page: " + str(i))
        reqStart = r"http://api.nytimes.com/svc/search/v2/articlesearch.json?q="
        reqPart2 = '&fq=news_desk:(' + searchTerm + ')' + '&document_type:("article")'
        reqPart3 = "&page=" + str(i) + "&sort=" + direction
        req = reqStart + reqPart2 + reqPart3 + "&api-key=" + key
        r = requests.get(req)

        if str(r) == "<Response [503]>":
            print("Service Unavailable!")
        else:
            try:
                data = r.json()

                for j in range(0,len(data["response"]["docs"])):
                    url = data["response"]["docs"][j]["web_url"]
                    doc = data["response"]["docs"][j]["_id"]

                    # convert date
                    try:
                        d = data["response"]["docs"][j]["pub_date"]
                        d = d.replace("Z","+0000")
                        tmp = datetime.datetime.strptime(d,"%Y-%m-%dT%H:%M:%S%z")
                        d2 = datetime.datetime.strftime(tmp,"%Y-%m-%d")
                    except ValueError as err:
                        print("Got a Value Error: ",err)
                        continue

                    df.loc[df.shape[0]] = ([doc,url,d2])

                # wait a second until making next request
                time.sleep(1)
            except ValueError as err:
                print("Got a Value Error: ",err)
                continue

    return df
#------------------------------------------------------------------------------
#                             SQL Functions
#------------------------------------------------------------------------------
def sqlQueryExecuter(engine,query):
    # make connection from engine
    con = engine.connect()
    # store tweets
    df = pd.read_sql(query, con = con)
    # close Connnection
    con.close()
    # return query result
    return df

def storeToDatabase(engine,info,table,key):
    # make connection to database from engine
    con = engine.connect()
    # get keys stored

    query = "SELECT " + key + " FROM " + table

    df = sqlQueryExecuter(engine,query)
    # remove duplicates
    info = info[~info.isin(df[key].tolist())[key]]
    # store tweets
    info.to_sql(name = table,
            con = con,
            if_exists = "append",
            index = False)

    # close Connnection
    con.close()

#------------------------------------------------------------------------------
#                           Script to run the functions
#------------------------------------------------------------------------------
if __name__ == "__main__":

    # get keys and login info for DB
    user = getpass.getuser()
    apiKey = loginInfo.getNYTimesAuth(user)
    dbAuth = loginInfo.getSQLLogin(user)

    # make connection to DataBase
    conURL = 'mysql+pymysql://' + dbAuth.username + ':' + dbAuth.password + '@localhost:3306/cse587?charset=utf8'
    engine = create_engine(conURL , echo=False)

    # get date to store
    d = datetime.datetime.now()
    today = d.strftime("%Y-%m-%d")
    try:
        for i in range(0,len(searchTable)):
            print("Getting URLS for search term " + str(i +1) + " from API...")
            data = getArticleURL(searchTerms[i],math.floor(pagesToScanThrough/len(searchTable)),direction,apiKey)
            print("Storing URLS to database...")
            storeToDatabase(engine,data,searchTable[i],"docID")
            print("Storing URLS to CSV...")
            q1 = "SELECT * FROM " + searchTable[i]+';'
            print(q1)
            df = sqlQueryExecuter(engine,q1)
            df.to_csv("../../data/" + searchTerms[i] + ".csv")
    except:
        print('There was a problem')
    print("Done.")
