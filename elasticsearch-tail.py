import datetime
import sys
import time as time2
from argparse import ArgumentParser
import threading
from random import randint
import platform
import re

try:
    from elasticsearch import Elasticsearch
except:
    print "ERROR: elasticsearch module not installed. Run 'sudo pip install elasticsearch'."
    exit(1)

# To-Do:
# ! Detect and break gracefully with Ctrl-C
# ! Keep time-in-the-past frozen when there are no new results are recover once they are to avoid gaps
# Include other fields (like 'level') as a searchable field on the main ES query
# Check the last-event-pointer going ahead overtime beyond the 10s boundary and adjust size of buffer
# Secondary sort of results by additional keys for events on the same timestamp
# Try/detect missing todays logstash index and go to the past searching for the latest one available
# Detect ES timeouts (in searching and in get_last_event)

# In case of error:
# "elasticsearch.exceptions.ConnectionError: ConnectionError(('Connection failed.', CannotSendRequest())) caused by: ConnectionError(('Connection failed.', CannotSendRequest()))"
# Update pip install --upgrade urllib3
# or
# Use a non HTTPS URL

# Arguments parsing
parser = ArgumentParser(description='Unix like tail command for Elastisearch.')
parser.add_argument('-e', '--endpoint', help='ES endpoint URL', required=True)
parser.add_argument('-t', '--type', help='Doc_Type: apache, java, tomcat,... ', default='apache')
parser.add_argument('-i', '--index', help='Index name. If none then logstash-%Y.%m.%d will be used.')
parser.add_argument('-c', '--host', help='Hostname to search (optional).')
parser.add_argument('-f', '--nonstop', help='Non stop. Continuous tailing.', action="store_true")
parser.add_argument('-n', '--docs', help='Number of documents.', default=10)
parser.add_argument('-s', '--showheaders', help='Show @timestamp, hostname and type fields in the output.', action="store_true")
parser.add_argument('-d', '--debug', help='Debug', action="store_true")

args = parser.parse_args()

def debug(message):
    if DEBUG:
        print "DEBUG " + str(message)

def normalize_endpoint(endpoint):
    end_with_number = re.compile(":\d+$")

    if endpoint[0:5] == "http:" and not end_with_number.search(endpoint):
        endpoint = endpoint+":80"
        return endpoint

    if endpoint[0:6] == "https:" and not end_with_number.search(endpoint):
        endpoint = endpoint+":443"
        return endpoint

    if not end_with_number.search(endpoint):
        endpoint = endpoint+":80"
        return endpoint

    return endpoint

### Args
# --endpoint
if args.endpoint == 'dummy' or args.endpoint == 'DUMMY':
    DUMMY = True
    print "INFO: Activating Dummy Load"
else:
    DUMMY = False
    endpoint = normalize_endpoint(args.endpoint)
# --type
doc_type = args.type
# --index
if not args.index:
    index = datetime.datetime.utcnow().strftime("logstash-%Y.%m.%d")
else:
    index = args.index
# --debug
if args.debug:
    DEBUG = args.debug
else:
    DEBUG = None
# --host
if args.host:
    host_to_search = args.host
else:
    host_to_search = ''
# --showheaders. Show @timestamp, hostname and type columns from the output.
if args.showheaders:
    show_headers = True
else:
    show_headers = False
# --nonstop
if args.nonstop:
    non_stop = True
else:
    non_stop = False
# -n --docs
docs = args.docs

###

# Workaround to make it work in AWS AMI Linux
# Python in AWS fails to locate the CA to validate the ES SSL endpoint and we need to specify it :(
# https://access.redhat.com/articles/2039753
if platform.platform()[0:5] == 'Linux':
    ca_certs = '/etc/pki/tls/certs/ca-bundle.crt'
else:
    # On the other side, in OSX works like a charm.
    ca_certs = None


def from_epoch_milliseconds_to_string(epoch_milli):
    return str(datetime.datetime.utcfromtimestamp( float(str( epoch_milli )[:-3]+'.'+str( epoch_milli )[-3:]) ).strftime('%Y-%m-%dT%H:%M:%S.%f'))[:-3]+"Z"


def from_epoch_seconds_to_string(epoch_secs):
    return from_epoch_milliseconds_to_string(epoch_secs * 1000)


def from_string_to_epoch_milliseconds(string):
    epoch = datetime.datetime(1970, 1, 1)
    pattern = '%Y-%m-%dT%H:%M:%S.%fZ'
    milliseconds =  int(str(int((datetime.datetime.strptime(from_date_time, pattern) - epoch).total_seconds()))+from_date_time[-4:-1])
    return milliseconds


def get_latest_event_timestamp(index):
    if DEBUG:
        current_time = int(datetime.datetime.utcnow().strftime('%s%f')[:-3])
    if host_to_search:
        res = es.search(size=1, index=index, doc_type=doc_type, fields="@timestamp", sort="@timestamp:desc",
                        body={
                            "query":
                                {"match_phrase": {"host": host_to_search}}
                        }
                        )
    else:
        res = es.search(size=1, index=index, doc_type=doc_type, fields="@timestamp", sort="@timestamp:desc",
                        body={
                            "query":
                                {"match_all": {}}
                        }
                        )

    debug("get_latest_event_timestamp "+str(res))

    # At least one event should return, otherwise we have an issue.
    # On To-Do: to go a logstash index back trying to find the last event (it might be midnight...)
    if len(res['hits']['hits']) != 0:
        timestamp = res['hits']['hits'][0]['sort'][0]
        # # Discard milliseconds and round down to seconds
        # timestamp = (timestamp/1000)*1000
        debug("get_latest_event_timestamp "+str(timestamp)+" "+from_epoch_milliseconds_to_string(timestamp))
        if DEBUG:
            debug("ES get_lastest_event execution time: " + str(int(datetime.datetime.utcnow().strftime('%s%f')[:-3]) - current_time) + "ms")
        return timestamp
    else:
        print "ERROR: get_latest_event_timestamp: No results"
        exit(1)


def get_latest_events(index): # And print them

    global event_pool
    global print_pool
    to_print = []

    if DEBUG:
        current_time = int(datetime.datetime.utcnow().strftime('%s%f')[:-3])
    if host_to_search:
        res = es.search(size=docs, index=index, doc_type=doc_type, fields="@timestamp,message,path,host",
                        sort="@timestamp:desc",
                        body={
                            "query":
                                {"match_phrase": {"host": host_to_search}}
                        }
                        )
    else:
        res = es.search(size=docs, index=index, doc_type=doc_type, fields="@timestamp,message,path,host",
                        sort="@timestamp:desc",
                        body={
                            "query":
                                {"match_all": {}}
                        }
                        )

    debug("get_latest_events" + str(res))

    # At least one event should return, otherwise we have an issue.
    # On To-Do: to go a logstash index back trying to find the last event (it might be midnight...)
    if len(res['hits']['hits']) != 0:
        timestamp = res['hits']['hits'][0]['sort'][0]

        to_object(res)

        #### Function needed here (and for purge() too)

        for event in event_pool:
            to_print.append(event_pool[event])

        # Sort by timestamp
        def getKey(item):
            return item['timestamp']

        for event in sorted(to_print, key=getKey):
            if show_headers:
                print_pool.append(
                    from_epoch_milliseconds_to_string(event['timestamp']) + " " + event['host'] + " " + event[
                        'type'] + " " + event['message'] + '\n')
            else:
                print_pool.append(event['message'] + '\n')

        what_to_do_while_we_wait()
        print_pool = []
        event_pool = {}

        ####

        debug("get_latest_event_timestamp " + str(timestamp) + " " + from_epoch_milliseconds_to_string(timestamp))
        if DEBUG:
            debug("ES get_lastest_events execution time: " + str(
                int(datetime.datetime.utcnow().strftime('%s%f')[:-3]) - current_time) + "ms")
        return timestamp
    else:
        print "ERROR: get_latest_events: No results"
        exit(1)


def get_latest_event_timestamp_dummy_load(index):
    # Return current time as fake lastest event in ES
    timestamp = int(datetime.datetime.utcnow().strftime('%s%f')[:-3]) - 20000

    # # Discard milliseconds and round down to seconds
    # timestamp = (timestamp/1000)*1000

    debug("get_latest_event_timestamp_dummy_load "+from_epoch_milliseconds_to_string(timestamp))
    return timestamp


def to_object(res):
    debug("to_object: in: len(event_pool) "+str(len(event_pool)))
    debug("to_object: hits: "+str(len(res['hits']['hits'])))

    for hit in res['hits']['hits']:
        host = str(hit['fields']['host'][0])
        id = str(hit['_id'])
        timestamp = str(hit['sort'][0])
        # host = str(hit['fields']['host'][0])
        # frontal = str(hit['fields']['frontal'][0])
        # message = hit['fields']['message'][0]
        # message = message.decode("unicode")
        try:
            # message = str(hit['fields']['message'][0])
            message = hit['fields']['message'][0]
        except:
            print "ERROR: *** ASCII out of range" + message + " ***"
            exit(1)

        # Every new event becomes a new key in the dictionary. Duplicated events (_id) cancel themselves (Only a copy remains)
        # In case an event is retrieved multiple times it won't cause duplicates.
        event_pool[id] = { 'timestamp': timestamp, 'host': host,'type': doc_type, 'message': message }
        # event_pool[id] = { 'timestamp': timestamp, 'frontal': frontal,'type': doc_type, 'message': message }

    debug("to_object: out: len(event_pool) "+str(len(event_pool)))
    return


# def get_oldest_in_the_pool(): # timestamp
#     if len(event_pool) != 0:
#         list = []
#         for event in event_pool:
#             #print event_pool[event]['timestamp']
#             # if event_pool[event]['timestamp'] <= oldest:
#             #     oldest = event_pool[event]['timestamp']
#             list.append(event_pool[event]['timestamp'])
#         oldest = int(sorted(list)[0])
#         debug("get_oldest_in_the_pool: "+str(oldest)+" "+from_epoch_milliseconds_to_string(int(oldest)))
#         return oldest


def purge_event_pool(event_pool):
    debug("purge_event_pool: in: "+str(len(event_pool)))
    # oldest = get_oldest_in_the_pool()
    debug("purge_event_pool: ten_seconds_ago "+from_epoch_milliseconds_to_string(ten_seconds_ago))

    to_print = []
    for event in event_pool.copy():
        # if str(event_pool[event]['timestamp'])[:-3] == oldest_seconds_string:
        event_timestamp = int(event_pool[event]['timestamp'])
        # if event_timestamp >= current time pointer and < (current time pointer + the gap covered by interval):
        # if event_timestamp >= ten_seconds_ago and (event_timestamp < ten_seconds_ago + interval):
        # if event_timestamp <= oldest_in_the_pool + interval:
        if (event_timestamp >= ten_seconds_ago - interval) and event_timestamp < ten_seconds_ago:
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

    # Print (add to print_pool) and let wait() function to print it out later
    for event in sorted(to_print,key=getKey):
        # print_event_by_event(event)
        if show_headers:
            print_pool.append(from_epoch_milliseconds_to_string(event['timestamp']) + " " + event['host'] + " " + event['type'] + " " + event['message'] + '\n')
            # print_pool.append(str(from_epoch_milliseconds_to_string(event['timestamp']) + " " + event['host'] + " " + event['type'] + " " + event['message']) + '\n')
        else:
            print_pool.append(event['message'] + '\n')
            # print_pool.append(str(event['message']) + '\n')
        # print_pool.append(str(from_epoch_milliseconds_to_string(event['timestamp']) + " " + event['host'] + " " + event['type'] + " " +event['message'])[0:width] + '\n')
        # print_pool.append(str(from_epoch_milliseconds_to_string(event['timestamp']) + " " + event['frontal'] + " " + event['type'] + " " +event['message'])[0:width] + '\n')

    debug("purge_event_pool: out: "+str(len(event_pool)))
    debug("purge_event_pool: len(print_pool) "+str(len(print_pool)))
    return


def query_test(from_date_time):
    res = es.search(size="30", index=index, doc_type=doc_type, fields="@timestamp,message,path,host",
                    sort="@timestamp:asc",
                    body={'query': {'match_phrase': {'host': host_to_search}}}
                    )
    return res


def search_events(from_date_time):
    if DEBUG:
        current_time = int(datetime.datetime.utcnow().strftime('%s%f')[:-3])
        debug("search_events: from_date_time: "+from_date_time)
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
    # http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search
    if host_to_search:
        debug("query: host: "+host_to_search)
        # res = es.search(size="10000", index=index, doc_type=doc_type, fields="@timestamp,message,path,host",
        #             sort="@timestamp:asc",
        #             body={
        #                 "query": {
        #                     "filtered": {
        #                         # "query": {"match": {"host": host}},
        #                         # "query": {"wildcard": {"host": "s*"}},
        #                         "query": {"match_phrase'": {"host": host_to_search}},
        #                         "filter": {
        #                             "range": {
        #                                 "@timestamp": {"gte": from_date_time}
        #                             }
        #                         }
        #                     }
        #                 }
        #             }
        #             )


        # res = es.search(size="10000", index=index, doc_type=doc_type, fields="@timestamp,message,path,host",
        #                 sort="@timestamp:asc",
        #                 body={
        #                     "query": {
        #                         "filtered": {
        #                             "filter": {
        #                                 "and": [
        #                                     {
        #                                         # "range":{
        #                                         #     "@timestamp":{"gte": from_date_time, "lte": to_date_time }
        #                                         # }
        #
        #                                         "range": {
        #                                             "@timestamp": {"gte": from_date_time}
        #                                         }
        #                                     },
        #                                     {
        #                                         # "term":{"_type": doc_type}
        #                                         "match_phrase": {"host": host_to_search }
        #                                         # "term": { "host": "s1" }
        #                                     }
        #
        #                                 ]
        #                             }
        #                         }
        #                     }
        #                 }
        #                 )

        res = es.search(size="10000", index=index, doc_type=doc_type, fields="@timestamp,message,path,host", sort="@timestamp:asc",
                        body={
                            "query": {
                                "filtered": {
                                    "query": {"match_phrase": {"host": host_to_search }},
                                    "filter": {
                                        "range": {
                                            "@timestamp": {"gte": from_date_time }
                                        }
                                    }
                                }
                            }
                        }
                        )

    else:
        res = es.search(size="10000", index=index, doc_type=doc_type, fields="@timestamp,message,path,host", sort="@timestamp:asc",
                        body={
                            "query": {
                                "filtered": {
                                    "filter": {
                                        "and": [
                                            {
                                                "range": {
                                                    "@timestamp": {"gte": from_date_time}
                                                }
                                            },
                                            {
                                                # "term":{"_type": doc_type}
                                                # "term": {"fields": {"host": "s1" } }
                                                # "term": { "host": "s1" }
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
        # Sleep just a bit to avoid hammering the CPU
        time2.sleep(.01)


def what_to_do_while_we_wait():
    global print_pool
    for i in range(0,len( print_pool) ):
        sys.stdout.write( print_pool[i] )
        sys.stdout.flush()
    print_pool = []


# def es_search(from_date_time):
#     # l.acquire()
#     # if current_process().name == 'Process-1':
#     # print "I'am", current_process().name
#     res = search_events(from_date_time)
#
#     debug("from_date_time "+from_date_time)
#     debug("hits: "+str(len(res['hits']['hits'])))
#
#     if len(res['hits']['hits']) == 0:
#         debug("Empty response!")
#     else:
#         # Add all the events in the response into the event_pool
#         to_object(res)


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
    # Let's simulate that ES takes some time to fulfill the request
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
        debug("Starting " + self.name)
        thread_execution(self.from_date_time)
        debug("Exiting " + self.name)
        del self



debug("main: now "+from_epoch_milliseconds_to_string(datetime.datetime.utcnow().strftime('%s%f')[:-3]))

interval = 1000 # milliseconds

# { "_id": {"timestamp":"sort(in milliseconds)", "host":"", "type":"", "message":"") }
event_pool = {}

print_pool = []

# docs = 10

to_the_past = 10000 # milliseconds

# http://elasticsearch-py.readthedocs.io/en/master/
if not DUMMY:
    es = Elasticsearch([endpoint],verify_certs=True, ca_certs=ca_certs)


# When not under -f just get the latest and exit
if non_stop == False:
    get_latest_events(index)
    exit(0)


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
# what_to_do_while_we_wait()
# ###

thread = Threading(1,"Thread-1", ten_seconds_ago)

while True:

    # From timestamp in milliseconds to Elasticsearch format (seconds.milliseconds). i.e: 2016-07-14T13:37:45.000Z
    from_date_time = from_epoch_milliseconds_to_string(ten_seconds_ago)

    if not thread.isAlive():
        thread = Threading(1,"Thread-1", from_date_time)
        thread.start()

    # Move the 'past' pointer one 'interval' ahead
    ten_seconds_ago += interval

    # Print and purge oldest events in the pool
    purge_event_pool(event_pool)

    # Wait for ES to index a bit more of stuff
    # time2.sleep(interval/1000)
    wait(interval)

    # And here we go again...
