import datetime
import time as time2
from elasticsearch import Elasticsearch

# http://elasticsearch-py.readthedocs.io/en/master/

es = Elasticsearch(['es:8080'])

index = "logstash-2016.07.08"
doc_type = "apache-access-az1"

# http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search
#res = es.search(size="20", index="logstash-2016.07.06", doc_type="apache-access-az2", fields="@timestamp,_id", body={"query": {"match": {"clientip": "66.249.64.165"}}})

def to_array(res):
    events = []
    for hit in res['hits']['hits']:
        # Event to array of tuples
        events.append( (hit['fields']['@timestamp'][0] , hit['fields']['message'][0]) )
        #print hit['fields']['@timestamp'][0],hit['fields']['message'][0]
    return events


def list_events(events):
    for event in events:
        print event


# def sort_event(events):
#     return


def get_latest_event_timestamp(index):
    res = es.search(size="1", index=index, doc_type=doc_type, fields="@timestamp,message", sort="@timestamp:desc", body={"query": {"match_all": {}}})
    timestamp = res['hits']['hits'][0]['fields']['@timestamp'][0]
    timestamp_formated = datetime.datetime( int(timestamp[0:4]), int(timestamp[5:7]), int(timestamp[8:10]), int(timestamp[11:13]), int(timestamp[14:16]), int(timestamp[17:19]))
    return timestamp_formated


latest_event_timestamp = get_latest_event_timestamp(index)
#get_latest_event_timestamp_formated = get_latest_event_timestamp[0:19]
#get_latest_event_timestamp = datetime.datetime( int(get_latest_event_timestamp[0:4]), int(get_latest_event_timestamp[5:7]), int(get_latest_event_timestamp[8:10]), int(get_latest_event_timestamp[11:13]), int(get_latest_event_timestamp[14:16]), int(get_latest_event_timestamp[17:19])   )

previous_event_timestamp = latest_event_timestamp

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

    latest_event_timestamp = get_latest_event_timestamp(index)
    one_second_ago = latest_event_timestamp - datetime.timedelta(seconds = 1)

    # from_date_time = six_days_ago_formated
    # to_date_time = six_days_ago_plus_one_second_formated

    from_date_time = one_second_ago.strftime('%Y-%m-%dT%H:%M:%S')
    to_date_time = latest_event_timestamp.strftime('%Y-%m-%dT%H:%M:%S')

    # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
    res = es.search(size="1000", index=index, doc_type=doc_type, fields="@timestamp,message", body={"query": {"range": {"@timestamp": {"gte": from_date_time, "lte": to_date_time }}}})

    events = to_array(res)
    # for event in events:
    #     print event

    list_events(events)
    #print len(events)

    previous_event_timestamp = one_second_ago

    time2.sleep(1)
