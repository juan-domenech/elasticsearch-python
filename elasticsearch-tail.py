import datetime
import sys
import time as time2
from argparse import ArgumentParser
from elasticsearch import Elasticsearch

DEBUG = None

# To-Do: Check the last-event-pointer going ahead overtime beyond the 10s boundary and adjust size of buffer

# Arguments parsing
parser = ArgumentParser(description='Unix like tail command for Elastisearch')
parser.add_argument('-e', '--endpoint', help='ES endpoint URL', default='es:80')
parser.add_argument('-t', '--type', help='Doc_Type: apache, java, tomcat,... ', default='apache')
parser.add_argument('-i', '--index', help='Index name. If none then logstash-%Y.%m.%d will be used.')
#parser.add_argument('-n', '--host', help='Hostname ', default='s1')
args = parser.parse_args()

# Elasticsearch endpoint
endpoint = args.endpoint
#
doc_type = args.type
#
# host = args.host
if not args.index:
    index = time2.strftime("logstash-%Y.%m.%d")
else:
    index = args.index


# { "_id": {"timestamp":"sort(in milliseconds)", "host":"", "type":"", "message":"") }
event_pool = {}

# http://elasticsearch-py.readthedocs.io/en/master/
# es = Elasticsearch(['es:8080'])
es = Elasticsearch(endpoint)

# http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search


def debug(message):
    if DEBUG:
        print "DEBUG "+str(message)


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
    debug("get_latest_event_timestamp "+str(timestamp)+str(datetime.datetime.utcfromtimestamp(timestamp/1000).strftime('%Y-%m-%dT%H:%M:%S.%f')))
    # timestamp_formated = datetime.datetime( int(timestamp[0:4]), int(timestamp[5:7]), int(timestamp[8:10]), int(timestamp[11:13]), int(timestamp[14:16]), int(timestamp[17:19]))
    return timestamp


# def to_array(res):
#     events = []
#     for hit in res['hits']['hits']:
#
#         id = str(hit['_id'])
#         # timestamp = str(hit['fields']['@timestamp'][0])
#         timestamp = str(hit['sort'][0])
#         # host = str(hit['fields']['host'][0])
#         frontal = str(hit['fields']['frontal'][0])
#         message = str(hit['fields']['message'][0])
#         # Event to array of tuples
#         #events.append( (hit['fields']['@timestamp'][0], hit['fields']['host'][0], hit['fields']['level'][0], hit['fields']['logmessage'][0]  ) )
#         # events.append((id, timestamp, host, doc_type, message))
#         events.append((id, timestamp, frontal, doc_type, message))
#
#         #print events
#         #print hit['fields']['@timestamp'][0],hit['fields']['message'][0]
#
#         # Every new event becomes a new key in the dictionary. Duplicated events (_id) cancel themselves (Only one remains)
#         # In case an event is retrieved multiple times won't cause duplicates.
#         # event_pool[id] = { 'timestamp': timestamp, 'host': host,'type': doc_type, 'message': message }
#         event_pool[id] = { 'timestamp': timestamp, 'host': frontal,'type': doc_type, 'message': message }
#
#     return events


def to_object(res):
    debug("into to_object len(event_pool) "+str(len(event_pool)))
    for hit in res['hits']['hits']:
        id = str(hit['_id'])
        timestamp = str(hit['sort'][0])
        # host = str(hit['fields']['host'][0])
        frontal = str(hit['fields']['frontal'][0])
        message = str(hit['fields']['message'][0])

        # Every new event becomes a new key in the dictionary. Duplicated events (_id) cancel themselves (Only one remains)
        # In case an event is retrieved multiple times won't cause duplicates.
        # event_pool[id] = { 'timestamp': timestamp, 'host': host,'type': doc_type, 'message': message }
        event_pool[id] = { 'timestamp': timestamp, 'host': frontal,'type': doc_type, 'message': message }
    debug("out of to_object len(event_pool) "+str(len(event_pool)))
    return


# def list_events(events):
#     for event in events:
#        print event[0], datetime.datetime.utcfromtimestamp( float(str( event[1] )[:-3]+'.'+str( event[1] )[-3:]) ).strftime('%Y-%m-%dT%H:%M:%S.%f'), event[2], event[3], event[4]


def print_event(event):
    event = event_pool[event]
    # print str(datetime.datetime.utcfromtimestamp( float(str( event['timestamp'] )[:-3]+'.'+str( event['timestamp'] )[-3:]) ).strftime('%Y-%m-%dT%H:%M:%S.%f'))[:-3]+"Z "+event['host']+" "+event['type']+" "+event['message']
    sys.stdout.write( str(datetime.datetime.utcfromtimestamp( float(str( event['timestamp'] )[:-3]+'.'+str( event['timestamp'] )[-3:]) ).strftime('%Y-%m-%dT%H:%M:%S.%f'))[:-3]+"Z "+event['host']+" "+event['type']+" "+event['message']+'\n' )
    sys.stdout.flush()


def purge_event_pool(event_pool):
    # ten_seconds_ago = datetime.datetime.utcnow() - datetime.timedelta(seconds = 10)
    # desired_time = ten_seconds_ago.strftime('%Y-%m-%dT%H:%M:%S')
    # print 'desired_time ',desired_time

    # print "DEBUG len(event_pool)",len(event_pool)
    #
    # # Get the oldest event timestamp
    # #oldest = 9999999999999
    # list = []
    # new_event_pool = {}
    # for event in event_pool:
    #     #print event_pool[event]['timestamp']
    #     # if event_pool[event]['timestamp'] <= oldest:
    #     #     oldest = event_pool[event]['timestamp']
    #     list.append(event_pool[event]['timestamp'])
    # oldest = sorted(list)[0]
    # print "DEBUG final oldest",oldest,datetime.datetime.utcfromtimestamp(float(oldest)/1000).strftime('%Y-%m-%dT%H:%M:%S.%f')

    debug("into purge len_pool "+str(len(event_pool)))
    oldest = get_oldest_in_the_pool(event_pool)

    # Print the events that are in the same second as the oldest event timestamp
    oldest_seconds = str(oldest)[:-3]
    # print "DEBUG oldest_seconds",oldest_seconds,datetime.datetime.utcfromtimestamp(float(oldest_seconds)).strftime('%Y-%m-%dT%H:%M:%S')
    debug("oldest_seconds "+oldest_seconds+datetime.datetime.utcfromtimestamp(float(oldest_seconds)).strftime('%Y-%m-%dT%H:%M:%S'))
    size = len(event_pool)
    for event in event_pool.copy():
        if str(event_pool[event]['timestamp'])[:-3] == oldest_seconds:
            # Print and...
            print_event(event)
            event_pool.pop(event)
        # else:
            # # Only copy what has not been printed out to have it ready for the next round
            # # debug(event_pool[event])
            # new_event_pool[event] = event_pool[event]

    debug("out of purge len_pool "+str(len(event_pool)))
    return


def get_oldest_in_the_pool(event_pool):
    # Get the oldest event timestamp
    list = []
    for event in event_pool:
        #print event_pool[event]['timestamp']
        # if event_pool[event]['timestamp'] <= oldest:
        #     oldest = event_pool[event]['timestamp']
        list.append(event_pool[event]['timestamp'])
    oldest = sorted(list)[0]
    debug("final oldest "+str(oldest)+(datetime.datetime.utcfromtimestamp(float(oldest)/1000).strftime('%Y-%m-%dT%H:%M:%S.%f')))

    return oldest


def search_events():
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
    res = es.search(size="9999", index=index, doc_type=doc_type, fields="@timestamp,message,path,frontal", sort="@timestamp:asc",
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
                                                }

                                        ]
                                    }
                            }
                        }
                    }
                    )
    return res


# Get oldest event timestamp from the Index
latest_event_timestamp = get_latest_event_timestamp(index)
# Substract on second from it
# one_second_ago = latest_event_timestamp - datetime.timedelta(seconds = 1)
# one_second_ago = latest_event_timestamp - 1000
# Go 10 senconds to the past
ten_seconds_ago = latest_event_timestamp - 10000

#get_latest_event_timestamp_formated = get_latest_event_timestamp[0:19]
#get_latest_event_timestamp = datetime.datetime( int(get_latest_event_timestamp[0:4]), int(get_latest_event_timestamp[5:7]), int(get_latest_event_timestamp[8:10]), int(get_latest_event_timestamp[11:13]), int(get_latest_event_timestamp[14:16]), int(get_latest_event_timestamp[17:19])   )
#print latest_event_timestamp
# previous_event_timestamp = latest_event_timestamp

while True:

    # current_time = datetime.datetime.now()
    # six_days_ago = current_time - datetime.timedelta(days = 5)
    # six_days_ago_formated = six_days_ago.strftime('%Y-%m-%dT%H:%M:%S')
    # six_days_ago_plus_one_second = six_days_ago + datetime.timedelta(seconds = 1)
    # six_days_ago_plus_one_second_formated = six_days_ago_plus_one_second.strftime('%Y-%m-%dT%H:%M:%S')

    # #print get_latest_event_timestamp[0:10]+" "+get_latest_event_timestamp[11:19]
    # print get_latest_event_timestamp[0:4], get_latest_event_timestamp[5:7], get_latest_event_timestamp[8:10]
    # print get_latest_event_timestamp[11:13], get_latest_event_timestamp[14:16], get_latest_event_timestamp[17:19]
    # exit(0)
    # get_latest_event_timestamp = datetime.datetime( int(get_latest_event_timestamp[0:4]), int(get_latest_event_timestamp[5:7]), int(get_latest_event_timestamp[8:10]), int(get_latest_event_timestamp[11:13]), int(get_latest_event_timestamp[14:16]), int(get_latest_event_timestamp[17:19])   )
    # print get_latest_event_timestamp

    # latest_event_timestamp = get_latest_event_timestamp(index)
    # one_second_ago = latest_event_timestamp - datetime.timedelta(seconds = 1)

    # from_date_time = six_days_ago_formated
    # to_date_time = six_days_ago_plus_one_second_formated

    #print str( latest_event_timestamp )[:-3]+'.'+str( latest_event_timestamp )[-3:]


    # From timestamp in milliseconds to Elasticsearch format (seconds.milliseconds)
    from_date_time = datetime.datetime.utcfromtimestamp( float(str( ten_seconds_ago )[:-3]+'.'+str( ten_seconds_ago )[-3:]) ).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+"Z"


    # to_date_time = datetime.datetime.utcfromtimestamp( float(str( latest_event_timestamp )[:-3]+'.'+str( latest_event_timestamp )[-3:]) ).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+"Z"

    # from_date_time = one_second_ago.strftime('%Y-%m-%dT%H:%M:%S')
    # to_date_time = latest_event_timestamp.strftime('%Y-%m-%dT%H:%M:%S')

    res = search_events()

    debug("from_date_time "+from_date_time)
    debug("hits: "+str(len(res['hits']['hits'])))

    if len(res['hits']['hits']) == 0:
        debug("Empty response!")
    else:

        to_object(res)

        purge_event_pool(event_pool)

        # oldest_in_the_pool = get_oldest_in_the_pool(event_pool)
        # print "DEBUG oldest_in_the_pool:",oldest_in_the_pool
        # # If our current pointer in the past + 10s is older than the
        # if ten_seconds_ago + 10000 < oldest_in_the_pool:
        #     debug("")

        # Move the 'present' to now (Epoch milliseconds)
        # latest_event_timestamp = datetime.datetime.utcnow().strftime('%s%f')[:-3]

        #print "*** latest_event_timestamp"+str(latest_event_timestamp)+"***"
        # latest_event_timestamp = get_latest_event_timestamp(index)
        # latest_event_timestamp = latest_event_timestamp - 1000

    # Move the 'past' pointer one second ahead
    ten_seconds_ago = ten_seconds_ago + 1000

    # Wait for ES to index a bit more of stuff
    time2.sleep(1)

    # And here we go again...