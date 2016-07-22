import datetime
from argparse import ArgumentParser
from elasticsearch import Elasticsearch

#
# Remove ALL _grokparsefailure documents
#

# To-Do:
# Add 'Are you sure?' question before deleting
# Bulk-delete

# Arguments parsing
parser = ArgumentParser(description='Remove ALL _grokparsefailure events')
parser.add_argument('-e', '--endpoint', help='ES endpoint URL', required=True)
parser.add_argument('-d', '--debug', help='Debug', action="store_true")
args = parser.parse_args()

if args.debug:
    DEBUG = args.debug
else:
    DEBUG = None

# Elasticsearch endpoint hostname:port
endpoint = args.endpoint

def debug(message):
    if DEBUG:
        print "DEBUG "+str(message)


def from_epoch_milliseconds_to_string(epoch_milli):
    return str(datetime.datetime.utcfromtimestamp( float(str( epoch_milli )[:-3]+'.'+str( epoch_milli )[-3:]) ).strftime('%Y-%m-%dT%H:%M:%S.%f'))[:-3]+"Z"


def from_epoch_seconds_to_string(epoch_secs):
    return from_epoch_milliseconds_to_string(epoch_secs * 1000)


def search_events():
    if DEBUG:
        current_time = int(datetime.datetime.utcnow().strftime('%s%f')[:-3])
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
    # http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search
    res = es.search(size="10000", fields="@timestamp,message,path,host",
                    body={
                        "query":{
                            "filtered":{
                                    "filter":{
                                        "and":[
                                            {
                                                # Optional
                                            },
                                            {
                                                "term":{"tags": '_grokparsefailure'}
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


def get_ids(res):
    ids = []
    for hit in res['hits']['hits']:
        id = str(hit['_id'])
        index = str(hit['_index'])
        type = str(hit['_type'])
        # [ ( id,index,type),... ]
        ids.append( (id,index,type) )
    return ids


def remove_ids(documents_to_remove):
    global deleted,errors
    # http://elasticsearch-py.readthedocs.io/en/master/api.html?highlight=delete#elasticsearch.Elasticsearch.delete
    for item in documents_to_remove:
        if DEBUG:
            current_time = int(datetime.datetime.utcnow().strftime('%s%f')[:-3])
        print "Deleting",item[0],"... ",
        # [ ( id,index,type),... ]
        delete = es.delete(id=item[0], index=item[1], doc_type=item[2])
        # if delete['_shards']['successful'] > 0 and delete['_shards']['failed'] == 0:
        if delete['found']:
            print "Successful!"
            deleted += 1
            debug("Deleted "+str(deleted))
        else:
            print "ERROR:",delete
            errors += 1
            debug("Errors " + str(errors))
        if DEBUG:
            debug(delete)
            debug("ES delete execution time: " + str(
                int(datetime.datetime.utcnow().strftime('%s%f')[:-3]) - current_time) + "ms")
    return

# http://elasticsearch-py.readthedocs.io/en/master/
es = Elasticsearch([endpoint],verify_certs=True)

res = search_events()

deleted = 0
errors = 0

print "Total documents found: "+str(len(res['hits']['hits']))
if len(res['hits']['hits']) == 0:
    print "Nothing to do."
    exit(0)

documents_to_remove = get_ids(res)

remove_ids(documents_to_remove)

print "Total Deleted:",deleted
if errors != 0:
    print "Total Errors:",errors
    exit(1)