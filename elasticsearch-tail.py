import datetime
import sys
import time as time2
from argparse import ArgumentParser
from elasticsearch import Elasticsearch
import threading
from random import randint

# To-Do:
# Check the last-event-pointer going ahead overtime beyond the 10s boundary and adjust size of buffer
# Eliminate the need of a sorted result from ES when searching
# Secondary sort of results by additional keys for events on the same timestamp
# Try/detect missing index and go to the past searching for the latest one

# Arguments parsing
parser = ArgumentParser(description='Unix like tail command for Elastisearch')
parser.add_argument('-e', '--endpoint', help='ES endpoint URL', default='es:80')
parser.add_argument('-t', '--type', help='Doc_Type: apache, java, tomcat,... ', default='apache')
parser.add_argument('-i', '--index', help='Index name. If none then logstash-%Y.%m.%d will be used.')
parser.add_argument('-d', '--debug', help='Debug', action="store_true")
#parser.add_argument('-n', '--host', help='Hostname ', default='s1')
args = parser.parse_args()

# Elasticsearch endpoint hostname:port
endpoint = args.endpoint
#
doc_type = args.type
#
# host = args.host
if not args.index:
    # index = time2.strftime("logstash-%Y.%m.%d")
    index = datetime.datetime.utcnow().strftime("logstash-%Y.%m.%d")
else:
    index = args.index
# debug = None or debug != None
if args.debug:
    DEBUG = args.debug
else:
    DEBUG = None
# Dummy Load
if args.endpoint == 'dummy' or args.endpoint == 'DUMMY':
    DUMMY = True
    print "Activating Dummy Load"
else:
    DUMMY = False


def debug(message):
    if DEBUG:
        print "DEBUG "+str(message)

def from_epoch_milliseconds_to_string(epoch_milli):
    return str(datetime.datetime.utcfromtimestamp( float(str( epoch_milli )[:-3]+'.'+str( epoch_milli )[-3:]) ).strftime('%Y-%m-%dT%H:%M:%S.%f'))[:-3]+"Z"

def from_epoch_seconds_to_string(epoch_secs):
    return from_epoch_milliseconds_to_string(epoch_secs * 1000)

def from_string_to_epoch_milliseconds(string):
    epoch = datetime.datetime(1970, 1, 1)
    pattern = '%Y-%m-%dT%H:%M:%S.%fZ'
    milliseconds =  int(str(int((datetime.datetime.strptime(from_date_time, pattern) - epoch).total_seconds()))+from_date_time[-4:-1])
    return milliseconds

# def print_event_by_key(event):
#     event = event_pool[event]
#     # print str(datetime.datetime.utcfromtimestamp( float(str( event['timestamp'] )[:-3]+'.'+str( event['timestamp'] )[-3:]) ).strftime('%Y-%m-%dT%H:%M:%S.%f'))[:-3]+"Z "+event['host']+" "+event['type']+" "+event['message']
#     # sys.stdout.write( str(datetime.datetime.utcfromtimestamp( float(str( event['timestamp'] )[:-3]+'.'+str( event['timestamp'] )[-3:]) ).strftime('%Y-%m-%dT%H:%M:%S.%f'))[:-3]+"Z "+event['host']+" "+event['type']+" "+event['message']+'\n' )
#     sys.stdout.write( from_epoch_milliseconds_to_string(event['timestamp'])+" "+event['host']+" "+event['type']+" "+event['message']+'\n' )
#     sys.stdout.flush()


# def print_event_by_event(event):
#     # sys.stdout.write( from_epoch_milliseconds_to_string(event['timestamp'])+" "+event['host']+" "+event['type']+" "+event['message']+'\n' )
#     # sys.stdout.flush()
#     print_pool.append(str(from_epoch_milliseconds_to_string(event['timestamp'])+" "+event['host']+" "+event['type']+" "+event['message'])[0:width]+'\n' )


def get_latest_event_timestamp(index):
    if DEBUG:
        current_time = int(datetime.datetime.utcnow().strftime('%s%f')[:-3])
    res = es.search(size="1", index=index, doc_type=doc_type, fields="@timestamp", sort="@timestamp:desc",
                    body={
                        "query":
                            {"match_all": {}
                             }
                    }
                    # body={
                    #     "query": {
                    #             "term": {
                    #                 "path": "/var/log/"
                    #             }
                    #     }
                    # }
                    )
    debug("get_latest_event_timestamp "+str(res))
    # timestamp = res['hits']['hits'][0]['fields']['@timestamp'][0]

    # Check empty response
    if len(res['hits']['hits']) != 0:
        timestamp = res['hits']['hits'][0]['sort'][0]
        # Discard milliseconds and round down to seconds
        timestamp = (timestamp/1000)*1000
        debug("get_latest_event_timestamp "+str(timestamp)+" "+from_epoch_milliseconds_to_string(timestamp))
        if DEBUG:
            debug("ES lastest_event execution time: " + str(int(datetime.datetime.utcnow().strftime('%s%f')[:-3]) - current_time) + "ms")
        return timestamp
    else:
        print "ERROR: get_latest_event_timestamp: No results"
        exit(1)


def get_latest_event_timestamp_dummy_load(index):
    # Return current time as fake lastest event in ES
    timestamp = int(datetime.datetime.utcnow().strftime('%s%f')[:-3]) - 20000

    # Discard milliseconds and round down to seconds
    timestamp = (timestamp/1000)*1000

    debug("get_latest_event_timestamp_dummy_load "+from_epoch_milliseconds_to_string(timestamp))
    return timestamp


def to_object(res):
    debug("to_object: in: len(event_pool) "+str(len(event_pool)))
    debug("to_object: hits: "+str(len(res['hits']['hits'])))
    for hit in res['hits']['hits']:
        id = str(hit['_id'])
        timestamp = str(hit['sort'][0])
        # host = str(hit['fields']['host'][0])
        frontal = str(hit['fields']['frontal'][0])
        message = str(hit['fields']['message'][0])

        # Every new event becomes a new key in the dictionary. Duplicated events (_id) cancel themselves (Only a copy remains)
        # In case an event is retrieved multiple times it won't cause duplicates.
        # event_pool[id] = { 'timestamp': timestamp, 'host': host,'type': doc_type, 'message': message }
        event_pool[id] = { 'timestamp': timestamp, 'frontal': frontal,'type': doc_type, 'message': message }
    debug("to_object: out: len(event_pool) "+str(len(event_pool)))
    return


def get_oldest_in_the_pool(): # timestamp
    if len(event_pool) != 0:
        list = []
        for event in event_pool:
            #print event_pool[event]['timestamp']
            # if event_pool[event]['timestamp'] <= oldest:
            #     oldest = event_pool[event]['timestamp']
            list.append(event_pool[event]['timestamp'])
        oldest = int(sorted(list)[0])
        debug("get_oldest_in_the_pool: "+str(oldest)+" "+from_epoch_milliseconds_to_string(int(oldest)))
        return oldest


def purge_event_pool(event_pool):
    debug("purge_event_pool: in: "+str(len(event_pool)))
    # oldest = get_oldest_in_the_pool()
    debug("purge_event_pool: ten_seconds_ago "+from_epoch_milliseconds_to_string(ten_seconds_ago))

    oldest_in_the_pool = get_oldest_in_the_pool()

    to_print = []
    for event in event_pool.copy():
        # if str(event_pool[event]['timestamp'])[:-3] == oldest_seconds_string:
        event_timestamp = int(event_pool[event]['timestamp'])
        # if event_timestamp >= current time pointer and < (current time pointer + the gap covered by interval):
        # if event_timestamp >= ten_seconds_ago and (event_timestamp < ten_seconds_ago + interval):
        # if event_timestamp <= oldest_in_the_pool + interval:
        if event_timestamp >= ten_seconds_ago - interval and event_timestamp < ten_seconds_ago:
            # Print and...
            to_print.append(event_pool[event])
            # delete...
            event_pool.pop(event)
        elif event_timestamp < ten_seconds_ago - interval:
            # ...discard what is below last output.
            event_pool.pop(event)

    # Sort by timestamp
    def getKey(item):
        return item['timestamp']

    # Print
    for event in sorted(to_print,key=getKey):
        # print_event_by_event(event)
        # print_pool.append(str(from_epoch_milliseconds_to_string(event['timestamp']) + " " + event['host'] + " " + event['type'] + " " +event['message'])[0:width] + '\n')
        print_pool.append(str(from_epoch_milliseconds_to_string(event['timestamp']) + " " + event['frontal'] + " " + event['type'] + " " +event['message'])[0:width] + '\n')

    debug("purge_event_pool: out: "+str(len(event_pool)))
    debug("purge_event_pool: len(print_pool) "+str(len(print_pool)))
    return


def search_events(from_date_time):
    if DEBUG:
        current_time = int(datetime.datetime.utcnow().strftime('%s%f')[:-3])
        debug("search_events: from_date_time: "+from_date_time)
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
    # http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search
    res = es.search(size="10000", index=index, doc_type=doc_type, fields="@timestamp,message,path,frontal", sort="@timestamp:asc",
                    body={
                        "query":{
                            "filtered":{
                                    "filter":{
                                        "and":[
                                            {
                                                # "range":{
                                                #     "@timestamp":{"gte": from_date_time, "lte": to_date_time }
                                                # }

                                                "range": {
                                                    "@timestamp": {"gte": from_date_time}
                                                }
                                            },
                                            {
                                                "term":{"_type": doc_type}
                                                # "term": {"_source": {"host": "s1" } }
                                                }

                                        ]
                                    }
                            }
                        }
                    }
                    )
    if DEBUG:
        debug("ES search execution time: "+str( int(datetime.datetime.utcnow().strftime('%s%f')[:-3]) - current_time)+"ms" )
    return res


def wait(milliseconds):
    # Current time in Epoch milliseconds
    current_time = int(datetime.datetime.utcnow().strftime('%s%f')[:-3])
    final_time = current_time + milliseconds
    # print "initial",current_time
    # print "final",final_time
    # skip = 0
    # start = 0
    # cumulative = 1
    # skip = float(milliseconds)/len(print_pool)
    # if len(print_pool) != 0:
    len_print_pool = len(print_pool)
    if len_print_pool == 0:
        # len_print_pool = 1
        debug("wait: len(print_pool) == 0")
    # jump = int(milliseconds)/len_print_pool+1
    while final_time > current_time:
        current_time = int(datetime.datetime.utcnow().strftime('%s%f')[:-3])
        what_to_do_while_we_wait()


def what_to_do_while_we_wait():
    global print_pool
    for i in range(0,len( print_pool) ):
        sys.stdout.write( print_pool[i] )
        sys.stdout.flush()
    print_pool = []


def es_search(from_date_time):
    # l.acquire()
    # if current_process().name == 'Process-1':
    # print "I'am", current_process().name
    res = search_events(from_date_time)

    debug("from_date_time "+from_date_time)
    debug("hits: "+str(len(res['hits']['hits'])))

    if len(res['hits']['hits']) == 0:
        debug("Empty response!")
    else:
        # Add all the events in the response into the event_pool
        to_object(res)

        # # Print and purge oldest events in the pool
        # purge_event_pool(event_pool)
    #
    #     # l.release()
    # else:
    #     print "I'am",current_process().name
    #     print "Process-1 already running!"



def search_events_dummy_load(from_date_time):
    """
    res = {
        u'hits':
               {u'hits': [
                   {u'sort': [1469265844000], u'_type': u'apache', u'_index': u'logstash-2016.07.23', u'_score': None, u'fields': {u'path': [u'/var/log/httpd/access_log'], u'host': [u'server-1.example.com'], u'message': [u'192.168.1.1, 192.168.1.2, 192.168.1.3 - - [23/Jul/2016:09:23:44 +0000] "GET /hello-world.html HTTP/1.1" 200 51 0/823 "-" "-"'], u'@timestamp': [u'2016-07-23T09:23:44.000Z']}, u'_id': u'AVYXEVBHthisisid1111'},
                   {u'sort': [1469265845000], u'_type': u'apache', u'_index': u'logstash-2016.07.23', u'_score': None, u'fields': {u'path': [u'/var/log/httpd/access_log'], u'host': [u'server-1.example.com'], u'message': [u'192.168.1.1, 192.168.1.2, 192.168.1.3 - - [23/Jul/2016:09:23:45 +0000] "GET /hello-world.html HTTP/1.1" 200 51 0/823 "-" "-"'], u'@timestamp': [u'2016-07-23T09:23:44.000Z']}, u'_id': u'AVYXEVBHthisisid1112'},
                   {u'sort': [1469265846000], u'_type': u'apache', u'_index': u'logstash-2016.07.23', u'_score': None, u'fields': {u'path': [u'/var/log/httpd/access_log'], u'host': [u'server-1.example.com'], u'message': [u'192.168.1.1, 192.168.1.2, 192.168.1.3 - - [23/Jul/2016:09:23:46 +0000] "GET /hello-world.html HTTP/1.1" 200 51 0/823 "-" "-"'], u'@timestamp': [u'2016-07-23T09:23:44.000Z']}, u'_id': u'AVYXEVBHthisisid1113'}
               ],
                   u'total': 3,
                   u'max_score': None
               },
        u'_shards': {u'successful': 5, u'failed': 0, u'total': 5},
        u'took': 376,
        u'timed_out': False
    }
    """
    debug("search_events_dummy_load: from_date_time: "+from_date_time)

    from_date_time_milliseconds = from_string_to_epoch_milliseconds(from_date_time)
    hits = []
    index = datetime.datetime.utcnow().strftime("logstash-%Y.%m.%d") # _index
    # _type
    score = None # _score
    host = 'server-1.example.com'
    path = '/var/log/httpd/access_log'
    message = '192.168.1.1, 192.168.1.2, 192.168.1.3 - - [23/Jul/2016:09:23:44 +0000] "GET /dummy.html HTTP/1.1" 200 51 0/823 "-" "-"'
    # @timestamp
    # _id
    # sort
    max_score = None
    shards = {'successful': 5, 'failed': 0, 'total': 5}
    took = 1000
    time_out = False

    total = 1000
    # total = randint(0,1000)
    debug('search_events_dummy_load: total: '+str(total))

    timestamp = from_date_time_milliseconds
    # fields = {'path': [path], 'host': [host], 'message': [message], '@timestamp': [from_epoch_milliseconds_to_string(timestamp)] }
    # hit = { 'sort': [timestamp], '_type': doc_type, '_index': index, '_score': score, 'fields': fields, '_id': doc_id }

    for i in range(0,total):

        doc_id = 'ES_DUMMY_ID_'+str(timestamp)[-8:]

        fields = {'path': [path], 'host': [host], 'message': [message], '@timestamp': [from_epoch_milliseconds_to_string(timestamp)] }
        hit = { 'sort': [timestamp], '_type': doc_type, '_index': index, '_score': score, 'fields': fields, '_id': doc_id }
        hits.append(hit)

        timestamp += 10

    hits = {'hits':hits}
    hits['total'] = total
    hits['max_score'] = max_score

    res = {'hits': hits}
    res['_shards'] = shards
    res['took'] = took
    res['time_out'] = time_out

    time2.sleep(2)

    return res


def thread_execution(from_date_time):

    if DUMMY:
        res = search_events_dummy_load(from_date_time)
    else:
        res = search_events(from_date_time)

    # Add all the events in the response into the event_pool
    to_object(res)

    return


class Threading (threading.Thread):
    def __init__(self, threadID, name, from_date_time):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.from_date_time = from_date_time
    def run(self):
        print "Starting " + self.name
        # print_time(self.name, 1, 5)
        thread_execution(self.from_date_time)
        print "Exiting " + self.name
        del self



debug("main: now "+from_epoch_milliseconds_to_string(datetime.datetime.utcnow().strftime('%s%f')[:-3]))

interval = 500

# { "_id": {"timestamp":"sort(in milliseconds)", "host":"", "type":"", "message":"") }
event_pool = {}

print_pool = []

width = 160

to_the_past = 10000 # milliseconds

# http://elasticsearch-py.readthedocs.io/en/master/
if not DUMMY:
    es = Elasticsearch(endpoint)

# Get the latest event timestamp from the Index
if DUMMY:
    latest_event_timestamp = get_latest_event_timestamp_dummy_load(index)
else:
    latest_event_timestamp = get_latest_event_timestamp(index)

# Go 10 seconds to the past. There is where we place "in the past" pointer to give time to ES to consolidate its index.
ten_seconds_ago = latest_event_timestamp - to_the_past

# ###
# # Initial load
# from_date_time = from_epoch_milliseconds_to_string(ten_seconds_ago)
# if DUMMY:
#     res = search_events_dummy_load(from_date_time)
# else:
#     res = search_events(from_date_time)
#
# debug("Initial load: from_date_time " + from_date_time)
# debug("Initial load: hits: " + str(len(res['hits']['hits'])))
#
# if len(res['hits']['hits']) == 0:
#     debug("Initial load: Empty response!")
# else:
#     # Add all the events in the response into the event_pool
#     to_object(res)
#
#     # Print and purge oldest events in the pool
#     purge_event_pool(event_pool)
#
# wait(interval)
# ###

thread = Threading(1,"Thread-1", ten_seconds_ago)

while True:

    # From timestamp in milliseconds to Elasticsearch format (seconds.milliseconds). i.e: 2016-07-14T13:37:45.000Z
    from_date_time = from_epoch_milliseconds_to_string(ten_seconds_ago)

    # if DUMMY:
    #     res = search_events_dummy_load(ten_seconds_ago)
    # else:
    #     res = search_events(ten_seconds_ago)
    #
    # # Add all the events in the response into the event_pool
    # to_object(res)

    if thread.isAlive() == False:
        thread = Threading(1,"Thread-1", from_date_time)
        thread.start()

    # Move the 'past' pointer one 'interval' ahead
    ten_seconds_ago = ten_seconds_ago + interval

    # Print and purge oldest events in the pool
    purge_event_pool(event_pool)

    # Wait for ES to index a bit more of stuff
    # time2.sleep(interval/1000)
    wait(interval)

    # And here we go again...