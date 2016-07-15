import datetime
import time as time2
from argparse import ArgumentParser
from elasticsearch import Elasticsearch

# Arguments parsing
parser = ArgumentParser(description='Unix like tail command for Elastisearch')
parser.add_argument('-e', '--endpoint', help='ES endpoint URL', default='es:80')
parser.add_argument('-t', '--type', help='Doc_Type: apache, java, tomcat,... ', default='apache')
#parser.add_argument('-n', '--host', help='Hostname ', default='s1')
args = parser.parse_args()

# Elasticsearch endpoint
endpoint = args.endpoint
#
doc_type = args.type
#
# host = args.host

# { "_id": {"timestamp":"sort(in milliseconds)", "host":"", "type":"", "message":"") }
event_pool = {}

# http://elasticsearch-py.readthedocs.io/en/master/
# es = Elasticsearch(['es:8080'])
es = Elasticsearch(endpoint)

index = time2.strftime("logstash-%Y.%m.%d")

# http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search

def to_array(res):
    events = []
    for hit in res['hits']['hits']:

        id = str(hit['_id'])
        # timestamp = str(hit['fields']['@timestamp'][0])
        timestamp = str(hit['sort'][0])
        host = str(hit['fields']['host'][0])
        message = str(hit['fields']['message'][0])
        # Event to array of tuples
        #events.append( (hit['fields']['@timestamp'][0], hit['fields']['host'][0], hit['fields']['level'][0], hit['fields']['logmessage'][0]  ) )
        events.append((id, timestamp, host, doc_type, message))

        #print hit['fields']['@timestamp'][0],hit['fields']['message'][0]

        # Every new event becomes a new key in the dictionary. Duplicated events (_id) cancel themselves.
        # This allows us to retrive the same event multiple times
        event_pool[id] = { 'timestamp': timestamp, 'host': host,'type': doc_type, 'message': message }

    return events


def list_events(events):
    for event in events:
        # print event[0], event[1], event[2], event[3], event[4]
        print event[0], datetime.datetime.utcfromtimestamp( float(str( event[1] )[:-3]+'.'+str( event[1] )[-3:]) ).strftime('%Y-%m-%dT%H:%M:%S.%f'), event[2], event[3], event[4]



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
    print "DEBUG:",res
    # timestamp = res['hits']['hits'][0]['fields']['@timestamp'][0]
    timestamp = res['hits']['hits'][0]['sort'][0]
    print "DEBUG:",timestamp
    # timestamp_formated = datetime.datetime( int(timestamp[0:4]), int(timestamp[5:7]), int(timestamp[8:10]), int(timestamp[11:13]), int(timestamp[14:16]), int(timestamp[17:19]))
    return timestamp


def print_event(event):
    event = event_pool[event]
    print "DEBUG:",event
    print datetime.datetime.utcfromtimestamp( float(str( event['timestamp'] )[:-3]+'.'+str( event['timestamp'] )[-3:]) ).strftime('%Y-%m-%dT%H:%M:%S.%f'), event['host'], event['type'], event['message']
    return


def purge_event_pool():
    # ten_seconds_ago = datetime.datetime.utcnow() - datetime.timedelta(seconds = 10)
    # desired_time = ten_seconds_ago.strftime('%Y-%m-%dT%H:%M:%S')
    # print 'desired_time ',desired_time

    # Get the oldest event timestamp
    #oldest = 9999999999999
    list = []
    for event in event_pool:
        #print event_pool[event]['timestamp']
        # if event_pool[event]['timestamp'] <= oldest:
        #     oldest = event_pool[event]['timestamp']
        list.append(event_pool[event]['timestamp'])
    oldest = sorted(list)[0]
    print "final oldest",oldest

    # Purge the events that are in the same second as the oldest event timestamp
    oldest_seconds = str(oldest)[:-3]
    for event in event_pool:
        if str(event_pool[event])[:-3] == oldest_seconds:
            # Print and...
            print_event(event)
            #my_dict.pop("key", None)
            # delete from the pool.
            event_pool.pop(event)



# Get oldest event timestamp from the Index
latest_event_timestamp = get_latest_event_timestamp(index)
# Substract on second from it
# one_second_ago = latest_event_timestamp - datetime.timedelta(seconds = 1)
one_second_ago = latest_event_timestamp - 1000
# That is what we need to get the first batch of events

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


    # From timestamp in milliseconds to Elasticsearch format
    # from_date_time = one_second_ago.strftime('%Y-%m-%dT%H:%M:%S')
    # to_date_time = latest_event_timestamp.strftime('%Y-%m-%dT%H:%M:%S')
    from_date_time = datetime.datetime.utcfromtimestamp( float(str( one_second_ago )[:-3]+'.'+str( one_second_ago )[-3:])  ).strftime('%Y-%m-%dT%H:%M:%S.%f')
    to_date_time = datetime.datetime.utcfromtimestamp( float(str( latest_event_timestamp )[:-3]+'.'+str( latest_event_timestamp )[-3:]) ).strftime('%Y-%m-%dT%H:%M:%S.%f')

    # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
    res = es.search(size="9999", index=index, doc_type=doc_type, fields="@timestamp,message,path,host,level", sort="@timestamp:asc",
                    body={
                        "query":{
                            "filtered":{
                                    "filter":{
                                        "and":[
                                            {
                                                "range":{
                                                    "@timestamp":{"gte": from_date_time, "lte": to_date_time }
                                                }

                                            },
                                            {
                                                "term":{"type": doc_type}
                                                }

                                        ]
                                    }
                            }
                        }
                    }
                    )

    events = to_array(res)
    # for event in events:
    #     print event

    list_events(events)
    #print len(events)
    #print event_pool
    #print len(event_pool)

    #purge_event_pool()

    # Move the 'past' pointer the the 'present' (the value that the previous get_latest_event_timestamp gave us
    one_second_ago = latest_event_timestamp

    # Wait for ES to index a bit more of stuff
    time2.sleep(1)

    # Move the present to the latest event found in ES
    latest_event_timestamp = get_latest_event_timestamp(index)
    # latest_event_timestamp = latest_event_timestamp - 1000

    # And here we go again...
