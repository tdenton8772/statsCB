# To change this license header, choose License Headers in Project Properties.
# To change this template file, choose Tools | Templates
# and open the template in the editor.

__author__ = "tdenton"
__date__ = "$Jan 30, 2017 5:13:29 PM$"

from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery
from couchbase.cluster import Cluster, ClassicAuthenticator, PasswordAuthenticator
import json
import urllib2

DEFAULT_URL = "couchbase://127.0.0.1"
DEFAULT_USER = "statsd_user"
DEFAULT_PW = "statsd"

class cb_bucket():
    def __init__(self, env):
        self.env = env
        self.get_buckets()
    def get_buckets(self):
        if self.env.upper() == "ERROR" or self.env.upper() == "LOC":
            self.statsd = Bucket('127.0.0.1/statsd', password='statsd')
        elif self.env.upper() == "DEV" or self.env.upper() == "TEST" or self.env.upper() == "STAGING" or self.env.upper() == "PROD":
            self.statsd = Bucket('127.0.0.1/statsd'.format(self.env, self.env), password='statsd')


            
class auth_cb_cluster():
    def __init__(self, env):
        self.DEFAULT_URL = "http://127.0.0.1"
        self.DEFAULT_USER = "Administrator"
        self.DEFAULT_PW = "statsd"
        self.env = env.upper()
        self.default_user = DEFAULT_USER
        self.default_pw = DEFAULT_PW
        self.default_url = DEFAULT_URL
        self.get_authenticated()
        
    def get_authenticated(self):
        if self.env.upper() == "ERROR" or self.env.upper() == "LOC":
                self.user = self.DEFAULT_USER
                self.passwrd = self.DEFAULT_PW
                self.url = self.DEFAULT_URL
        else:
            if self.env == "DEV":
                self.url = "couchbase://127.0.0.1"
                self.user = "statsd_user"
                self.passwrd = "statsd_user"
            if self.env == "TEST": 
                self.url = "couchbase://127.0.0.1"
                self.user = "statsd_user"
                self.passwrd = "statsd_user"
            if self.env == "STAGING": 
                self.url = "couchbase://127.0.0.1"
                self.user = "statsd_user"
                self.passwrd = "statsd_user"
            if self.env == "PROD": 
                self.url = "couchbase://127.0.0.1"
                self.user = "statsd_user"
                self.passwrd = "statsd_user"
        
        self.version = self.get_version()
        print(self.version)
        if self.version >= 5:
            self.cluster = Cluster('{}'.format(self.url))         
            self.authenticator = PasswordAuthenticator(self.user, self.passwrd)
            self.cluster.authenticate(self.authenticator)
            self.statsd = self.cluster.open_bucket('statsD')
            
        else:
            self.cluster = Cluster('{}'.format(self.url)) 
            self.cluster.authenticate(ClassicAuthenticator(buckets={'statsd_user':'statsd'}))
            self.statsd = self.cluster.open_bucket('statsd')

            
    def basic_authorization(self, user, password):
        s = user + ":" + password
        return "Basic " + s.encode("base64").rstrip()

    def get_version(self):
        try:
            url = "{}:8091/pools".format(self.url)
            req = urllib2.Request(url,
                                  headers={
                                  "Authorization": self.basic_authorization(self.user, self.passwrd),
                                  "Content-Type": "application/x-www-form-urlencoded",

                                  # Some extra headers for fun
                                  "Accept": "*/*", # curl does this
                                  "User-Agent": "check_version/1", # otherwise it uses "Python-urllib/..."
                                  })

            f = (urllib2.urlopen(req)).read()
            f_json = json.loads(f)
            version = float("{}.{}".format(f_json['implementationVersion'].split(".")[0], f_json['implementationVersion'].split(".")[1]))
            return(version)
        except Exception as e:
            print("Curl Error:", e.args)
            return(4.5)
            
def get_class_bucket(env):
    instance = cb_bucket(env)
    return instance

def get_bucket(env, bkt):
    print env, bkt.upper()
    if bkt.upper() == "statsd":
        if env.upper() == "ERROR":
            cb = Bucket('{}/statsd', password='statsd')
        elif env.upper() == "DEV":
            cb = Bucket('{}/statsd', password='statsd')
        elif env.upper() == "LOC":
            cb = Bucket('{}/statsd', password='statsd')
        elif env.upper() == "TEST":
            cb = Bucket('{}/statsd', password='statsd')
        elif env.upper() == "STAGING":
            cb = Bucket('{}/statsd', password='statsd')
        elif env.upper() == "PROD":
            cb = Bucket('{}/statsd', password='statsd')
    print "Got Bucket"
    return cb

def get_cluster(env):
    instance = auth_cb_cluster(env)
    return instance
        