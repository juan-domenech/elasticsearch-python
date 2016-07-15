import datetime
import time as time2
from argparse import ArgumentParser
from elasticsearch import Elasticsearch

# Arguments parsing
parser = ArgumentParser(description='Unix like tail command for Elastisearch')
parser.add_argument('-e', '--endpoint', help='ES endpoint URL', default='es:80')
parser.add_argument('-t', '--type', help='Doc_Type: apache, java, tomcat,... ', default='apache')
#parser.add_argument('-n', '--host', help='Hostname ', default='s1-ejs-b-euw.ej.mttnow.com')
args = parser.parse_args()

# Elasticsearch endpoint
endpoint = args.endpoint
#
doc_type = args.type
#
# host = args.host

# http://elasticsearch-py.readthedocs.io/en/master/
# es = Elasticsearch(['es:8080'])
es = Elasticsearch(endpoint)

index = time2.strftime("logstash-%Y.%m.%d")

# http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search

def to_array(res):
    events = []
    for hit in res['hits']['hits']:
        # Event to array of tuples
        #events.append( (hit['fields']['@timestamp'][0], hit['fields']['host'][0], hit['fields']['level'][0], hit['fields']['logmessage'][0]  ) )
        events.append((hit['fields']['@timestamp'][0], hit['fields']['host'][0], hit['fields']['message'][0]))

        #print hit['fields']['@timestamp'][0],hit['fields']['message'][0]
    return events


def list_events(events):
    for event in events:
        print event[0],event[1], event[2]


# def sort_event(events):
#     return


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
    timestamp = res['hits']['hits'][0]['fields']['@timestamp'][0]
    print "DEBUG:",timestamp
    timestamp_formated = datetime.datetime( int(timestamp[0:4]), int(timestamp[5:7]), int(timestamp[8:10]), int(timestamp[11:13]), int(timestamp[14:16]), int(timestamp[17:19]))
    return timestamp_formated

# Check when was the last event
latest_event_timestamp = get_latest_event_timestamp(index)
# Substract on second from it
one_second_ago = latest_event_timestamp - datetime.timedelta(seconds = 1)
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



    # Formated to work with datetime
    from_date_time = one_second_ago.strftime('%Y-%m-%dT%H:%M:%S')
    to_date_time = latest_event_timestamp.strftime('%Y-%m-%dT%H:%M:%S')

    # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
    res = es.search(size="1000", index=index, doc_type=doc_type, fields="@timestamp,message,path,host,level", sort="@timestamp:asc",
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


    # Move the 'past' pointer the the 'present' (the value that the previous get_latest_event_timestamp gave us
    one_second_ago = latest_event_timestamp

    # Wait for ES to index a bit more of stuff
    time2.sleep(1)

    # Move the present to the latest event found in ES
    latest_event_timestamp = get_latest_event_timestamp(index)

    # And here we go again...
