# Author(s): Joseph Hadley, Jonathan Hunt
# Created : 2018-22-03
# Modified: 2018-23-04
# Description: this file keeps all of the api keys in one place out of the data
#   gathering scripts
#------------------------------------------------------------------------------
# class to get/store authentication keys
class AuthKeys:
    def __init__(self, consumerKey,consumerSecret,accessToken,accessTokenSecret):
        self.consumerKey = consumerKey
        self.consumerSecret = consumerSecret
        self.accessToken = accessToken
        self.accessTokenSecret = accessTokenSecret

class MySQLLogin:
    def __init__(self, username, password):
        self.username = username
        self.password = password
#------------------------------------------------------------------------------
#                             Hadley keys/sql login
#------------------------------------------------------------------------------
joeTwitter = AuthKeys("zGI5Yljg3Xhb7QAANM76jQs2d",
                        "2xay2iQQu5AOXeiPpuHuLCri1vLmjFRD7i3iIkRDC46bdLQ9IY",
                        "960574817178177536-YG2l47flaQQB7DDLpwqLeYa2rDu8oIw",
                        "yDefhVjCSOnTIs4U99TEF2fNu1Y20oKVipUSwQvsJVyHo")

joeNYT = AuthKeys("bc22f215df24440dbef35cef947d0461",
                None,
                None,
                None)

joeSQL = MySQLLogin("root", "Hadleyj1")
#------------------------------------------------------------------------------
#                               Hunt keys
#------------------------------------------------------------------------------
jonNYT = AuthKeys("40c1831f84e54c43aea28695d845df76",
                None,
                None,
                None)

jonSQL = MySQLLogin("root","17goals")
#------------------------------------------------------------------------------
#                          Functions to get info
#------------------------------------------------------------------------------
def getTwitterAuth(user):
    if user == 'jkhadley':
        return joeTwitter
    elif user == 'jthunt':
        return jonTwitter
    else:
        print("No user: " + str(user))

def getNYTimesAuth(user):
    if user == 'jkhadley':
        return joeNYT.consumerKey
    elif user == 'jonathanhunt':
        return jonNYT.consumerKey
    else:
        print("No user: " + str(user))

def getSQLLogin(user):
    if user == 'jkhadley':
        return joeSQL
    elif user == 'jonathanhunt':
        return jonSQL
    else:
        print("No user: " + str(user))
