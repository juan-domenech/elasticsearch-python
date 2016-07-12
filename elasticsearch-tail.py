import datetime
import time as time2
from elasticsearch import Elasticsearch

# time = datetime.datetime.now()
# six_days_ago = time - datetime.timedelta(days = 6)
# six_days_ago_format = six_days_ago.strftime('%Y-%m-%dT%H:%M:%S:010Z')
# six_days_ago_plus_one_second = six_days_ago + datetime.timedelta(seconds = 1)
# six_days_ago_plus_one_second_format = six_days_ago_plus_one_second.strftime('%Y-%m-%dT%H:%M:%S:010Z')

# print six_days_ago_format
# print six_days_ago_plus_one_second_format
# exit()

# http://elasticsearch-py.readthedocs.io/en/master/

es = Elasticsearch(['es:8080'])

# http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search
#res = es.search(size="20", index="logstash-2016.07.06", doc_type="apache-access-az2", fields="@timestamp,_id", body={"query": {"match": {"clientip": "66.249.64.165"}}})

while True:

    time = datetime.datetime.now()
    six_days_ago = time - datetime.timedelta(days = 6)
    six_days_ago_format = six_days_ago.strftime('%Y-%m-%dT%H:%M:%S')
    six_days_ago_plus_one_second = six_days_ago + datetime.timedelta(seconds = 1)
    six_days_ago_plus_one_second_format = six_days_ago_plus_one_second.strftime('%Y-%m-%dT%H:%M:%S')

    # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
    res = es.search(size="1000", index="logstash-2016.07.06", doc_type="apache-access-az2", fields="@timestamp,message", body={ "query": { "range": { "@timestamp": { "gte": six_days_ago_format, "lte": six_days_ago_plus_one_second_format }}}})

    for hit in res['hits']['hits']:
        print hit['fields']['@timestamp'],hit['fields']['message'][0]

    time2.sleep(1)
