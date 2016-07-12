import datetime
import time as time2
from elasticsearch import Elasticsearch

# http://elasticsearch-py.readthedocs.io/en/master/

es = Elasticsearch(['es:8080'])

index = "logstash-2016.07.07"
doc_type = "apache-access-az2"

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
    return res['hits']['hits'][0]['fields']['@timestamp'][0]


get_latest_event_timestamp = get_latest_event_timestamp(index)
get_latest_event_timestamp_formated = get_latest_event_timestamp[0:19]


#exit()



while True:

    current_time = datetime.datetime.now()
    six_days_ago = current_time - datetime.timedelta(days = 5)
    six_days_ago_formated = six_days_ago.strftime('%Y-%m-%dT%H:%M:%S')
    six_days_ago_plus_one_second = six_days_ago + datetime.timedelta(seconds = 1)
    six_days_ago_plus_one_second_formated = six_days_ago_plus_one_second.strftime('%Y-%m-%dT%H:%M:%S')

    # print current_time
    # print get_latest_event_timestamp
    # #print get_latest_event_timestamp[0:10]+" "+get_latest_event_timestamp[11:19]
    # one_second_ago = get_latest_event_timestamp[0:10]+" "+get_latest_event_timestamp[11:19] - datetime.timedelta(seconds = 1)
    # print get_latest_event_timestamp_formated
    # print one_second_ago
    #
    # exit(0)
    # index = 'logstash-'+six_days_ago_formated[0:11]
    # print index

    from_date_time = six_days_ago_formated
    to_date_time = six_days_ago_plus_one_second_formated

    print from_date_time
    print to_date_time

    # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
    res = es.search(size="1000", index=index, doc_type=doc_type, fields="@timestamp,message", body={"query": {"range": {"@timestamp": {"gte": from_date_time, "lte": to_date_time }}}})

    # events = []
    # for hit in res['hits']['hits']:
    #     # Event to array of tuples
    #     events.append( (hit['fields']['@timestamp'][0] , hit['fields']['message'][0]) )
    #     #print hit['fields']['@timestamp'][0],hit['fields']['message'][0]

    events = to_array(res)
    # for event in events:
    #     print event

    list_events(events)
    #print len(events)

    time2.sleep(1)
