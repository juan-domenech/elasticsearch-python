import datetime
import sys
import time as time2
from argparse import ArgumentParser
from elasticsearch import Elasticsearch
from multiprocessing import Process, Lock, current_process

# To-Do:
# Check the last-event-pointer going ahead overtime beyond the 10s boundary and adjust size of buffer
# Eliminate the need of a sorted result from ES when searching
# Secondary sort of results by additional keys for events on the same timestamp


# Arguments parsing
parser = ArgumentParser(description='Unix like tail command for Elastisearch')
parser.add_argument('-e', '--endpoint', help='ES endpoint URL', default='es:80')
parser.add_argument('-t', '--type', help='Doc_Type: apache, java, tomcat,... ', default='apache')
parser.add_argument('-i', '--index', help='Index name. If none then logstash-%Y.%m.%d will be used.')
parser.add_argument('-d', '--debug', help='Debug')
#parser.add_argument('-n', '--host', help='Hostname ', default='s1')
args = parser.parse_args()

# Elasticsearch endpoint hostname:port
endpoint = args.endpoint
#
doc_type = args.type
#
# host = args.host
if not args.index:
    index = time2.strftime("logstash-%Y.%m.%d")
else:
    index = args.index
# debug = None or debug != None
if args.debug:
    DEBUG = args.debug
else:
    DEBUG = None



def debug(message):
    if DEBUG:
        print "DEBUG "+str(message)


def from_epoch_milliseconds_to_string(epoch_milli):
    return str(datetime.datetime.utcfromtimestamp( float(str( epoch_milli )[:-3]+'.'+str( epoch_milli )[-3:]) ).strftime('%Y-%m-%dT%H:%M:%S.%f'))[:-3]+"Z"


def from_epoch_seconds_to_string(epoch_secs):
    return from_epoch_milliseconds_to_string(epoch_secs * 1000)


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

    timestamp = res['hits']['hits'][0]['sort'][0]
    # Discard milliseconds and round down to seconds
    timestamp = (timestamp/1000)*1000

    debug("get_latest_event_timestamp "+str(timestamp)+" "+from_epoch_milliseconds_to_string(timestamp))
    return timestamp


def to_object(res):
    debug("into to_object len(event_pool) "+str(len(event_pool)))
    for hit in res['hits']['hits']:
        id = str(hit['_id'])
        timestamp = str(hit['sort'][0])
        host = str(hit['fields']['host'][0])
        # frontal = str(hit['fields']['frontal'][0])
        message = str(hit['fields']['message'][0])

        # Every new event becomes a new key in the dictionary. Duplicated events (_id) cancel themselves (Only a copy remains)
        # In case an event is retrieved multiple times it won't cause duplicates.
        event_pool[id] = { 'timestamp': timestamp, 'host': host,'type': doc_type, 'message': message }
        # event_pool[id] = { 'timestamp': timestamp, 'host': frontal,'type': doc_type, 'message': message }
    debug("out of to_object len(event_pool) "+str(len(event_pool)))
    return


def purge_event_pool(event_pool):
    debug("into purge len_pool "+str(len(event_pool)))
    # oldest = get_oldest_in_the_pool()

    debug("purge: ten_seconds_ago "+from_epoch_milliseconds_to_string(ten_seconds_ago))
    to_print = []
    for event in event_pool.copy():
        # if str(event_pool[event]['timestamp'])[:-3] == oldest_seconds_string:
        event_timestamp = int(event_pool[event]['timestamp'])
        # if event_timestamp >= current time pointer and < (current time pointer + the gap covered by interval):
        if event_timestamp >= ten_seconds_ago and (event_timestamp < ten_seconds_ago + interval):
            # Print and...
            to_print.append(event_pool[event])
            # delete.
            event_pool.pop(event)
        # else:
        #     debug("ignored event_timestamp "+str(from_epoch_milliseconds_to_string(event_timestamp)))

    # Sort by timestamp
    def getKey(item):
        return item['timestamp']

    # Print
    for event in sorted(to_print,key=getKey):
        # print_event_by_event(event)
        print_pool.append(str(from_epoch_milliseconds_to_string(event['timestamp']) + " " + event['host'] + " " + event['type'] + " " +event['message'])[0:width] + '\n')

    debug("out of purge len_pool "+str(len(event_pool)))
    debug("len(print_pool)"+str(len(print_pool)))
    return


# def get_oldest_in_the_pool(): # timestamp
#     list = []
#     for event in event_pool:
#         #print event_pool[event]['timestamp']
#         # if event_pool[event]['timestamp'] <= oldest:
#         #     oldest = event_pool[event]['timestamp']
#         list.append(event_pool[event]['timestamp'])
#     oldest = int(sorted(list)[0])
#     debug("oldest "+str(oldest)+" "+from_epoch_milliseconds_to_string(int(oldest)))
#     return oldest


def search_events(from_date_time):
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
    # http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search
    res = es.search(size="10000", index=index, doc_type=doc_type, fields="@timestamp,message,path,host", sort="@timestamp:asc",
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
    return res


def wait(milliseconds):
    # Current time in Epoch milliseconds
    current_time = int(datetime.datetime.utcnow().strftime('%s%f')[:-3])
    final_time = current_time + milliseconds
    # print "initial",current_time
    # print "final",final_time

    # start = 0
    # cumulative = 1
    # skip = float(milliseconds)/len(print_pool)
    while final_time > current_time:
        current_time = int(datetime.datetime.utcnow().strftime('%s%f')[:-3])
        # print len(print_pool)
        if len(print_pool) != 0:
            sys.stdout.write( print_pool[0] )
            sys.stdout.flush()
            print_pool.pop(0)



def es_search(from_date_time):
    # l.acquire()
    # if current_process().name == 'Process-1':
    print "I'am", current_process().name
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



interval = 500

# { "_id": {"timestamp":"sort(in milliseconds)", "host":"", "type":"", "message":"") }
event_pool = {}

print_pool = []

width = 160

# http://elasticsearch-py.readthedocs.io/en/master/
es = Elasticsearch(endpoint)

# Get the latest event timestamp from the Index
latest_event_timestamp = get_latest_event_timestamp(index)

# Go 10 seconds to the past. There is where we place "in the past" pointer to give time to ES to consolidate its index.
ten_seconds_ago = latest_event_timestamp - 10000

###
# Initial load
from_date_time = from_epoch_milliseconds_to_string(ten_seconds_ago)
res = search_events(from_date_time)

debug("Initial load: from_date_time " + from_date_time)
debug("Initial load: hits: " + str(len(res['hits']['hits'])))

if len(res['hits']['hits']) == 0:
    debug("Initial load: Empty response!")
else:
    # Add all the events in the response into the event_pool
    to_object(res)

    # # Print and purge oldest events in the pool
    purge_event_pool(event_pool)

wait(interval)
###

while True:

    # From timestamp in milliseconds to Elasticsearch format (seconds.milliseconds). i.e: 2016-07-14T13:37:45.000Z
    # from_date_time = datetime.datetime.utcfromtimestamp( float(str( ten_seconds_ago )[:-3]+'.'+str( ten_seconds_ago )[-3:]) ).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+"Z"
    from_date_time = from_epoch_milliseconds_to_string(ten_seconds_ago)

    # to_date_time = datetime.datetime.utcfromtimestamp( float(str( latest_event_timestamp )[:-3]+'.'+str( latest_event_timestamp )[-3:]) ).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+"Z"

    # from_date_time = one_second_ago.strftime('%Y-%m-%dT%H:%M:%S')
    # to_date_time = latest_event_timestamp.strftime('%Y-%m-%dT%H:%M:%S')


    # res = search_events(from_date_time)
    p = Process(target=es_search, args=(from_date_time,))
    if __name__ == '__main__':
        # lock = Lock()
        if not p.is_alive():
            p.start()
            # p.join()


    # debug("from_date_time "+from_date_time)
    # debug("hits: "+str(len(res['hits']['hits'])))
    #
    # if len(res['hits']['hits']) == 0:
    #     debug("Empty response!")
    #
    # else:
    #     # Add all the events in the response into the event_pool
    #     to_object(res)
    #
    #     # Print and purge oldest events in the pool
    #     purge_event_pool(event_pool)


    # Print and purge oldest events in the pool
    purge_event_pool(event_pool)

    # Move the 'present' to now (Epoch milliseconds)
    # latest_event_timestamp = datetime.datetime.utcnow().strftime('%s%f')[:-3]

    # Move the 'past' pointer one 'interval' ahead
    ten_seconds_ago = ten_seconds_ago + interval

    # Wait for ES to index a bit more of stuff
    # time2.sleep(interval/1000)
    wait(interval)

    # And here we go again...