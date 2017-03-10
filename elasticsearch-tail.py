import datetime
import sys
import time as time2
from argparse import ArgumentParser
import threading
# from random import randint
import platform
import re
import signal # Dealing with Ctrl+C
import codecs, locale # Dealing with Unicode
import hashlib

try:
    from elasticsearch import Elasticsearch
except:
    print "ERROR: elasticsearch module not installed. Please run 'sudo pip install elasticsearch'."
    sys.exit(1)

# To-Do:
# ! Intial load + printing
# Detect certificate failure and show error (and ignore?)
# ! Keep time-in-the-past frozen when there are no new results are recover once they appear to avoid potential gaps
# Check the last-event-pointer going ahead overtime beyond the 10s boundary and move pointer accordingly
# Midnight scenario
# Detect ES timeouts and missing shards (in searching and in get_last_event)
# Improve dummy-load: Timestamp in standard format, Apache number of docs variable when not -f, random number of bytes, --type java,
# Solve the 10000 limit on the answer from ES

# In case of error:
# "elasticsearch.exceptions.ConnectionError: ConnectionError(('Connection failed.', CannotSendRequest())) caused by: ConnectionError(('Connection failed.', CannotSendRequest()))"
# Update pip install --upgrade urllib3
# or use a non HTTPS Endpoint URL

# Arguments parsing
parser = ArgumentParser(description='Unix like tail command for Elastisearch and Logstash.')
parser.add_argument('-e', '--endpoint', help='ES endpoint URL.', required=True)
parser.add_argument('-t', '--type', help='Doc_Type: apache, java, tomcat,... ', default='apache')
parser.add_argument('-i', '--index', help='Index name. If none then "logstash-YYYY.MM.DD" will be used.')
parser.add_argument('-o', '--hostname', help='Hostname to search (optional).')
parser.add_argument('-l', '--javalevel', help='Java Level.')
parser.add_argument('-j', '--javaclass', help='Java Class.')
parser.add_argument('-r', '--httpresponse', help='HTTP Server Response.')
parser.add_argument('-m', '--httpmethod', help='HTTP Request Method.')
parser.add_argument('-f', '--nonstop', help='Non stop. Continuous tailing.', action="store_true")
parser.add_argument('-n', '--docs', help='Number of documents.', default=10)
parser.add_argument('-s', '--showheaders', help='Show @timestamp, hostname and type fields in the output.', action="store_true")
parser.add_argument('-d', '--debug', help='Debug', action="store_true")
args = parser.parse_args()

# Dealing with Unicode
sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)

# Ctrl+C
def signal_handler(signal, frame):
    debug('Ctrl+C pressed!')
    sys.exit(0)


def debug(message):
    if DEBUG:
        print "DEBUG " + str(message)


def normalize_endpoint(endpoint):
    end_with_number = re.compile(":\d+$")

    if endpoint[-1:] == '/':
        endpoint = endpoint[:-1]

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


def from_epoch_milliseconds_to_string(epoch_milli):
    return str(datetime.datetime.utcfromtimestamp( float(str( epoch_milli )[:-3]+'.'+str( epoch_milli )[-3:]) ).strftime('%Y-%m-%dT%H:%M:%S.%f'))[:-3]+"Z"


# def from_epoch_seconds_to_string(epoch_secs):
#     return from_epoch_milliseconds_to_string(epoch_secs * 1000)


def from_string_to_epoch_milliseconds(string):
    epoch = datetime.datetime(1970, 1, 1)
    pattern = '%Y-%m-%dT%H:%M:%S.%fZ'
    # milliseconds =  int(str(int((datetime.datetime.strptime(from_date_time, pattern) - epoch).total_seconds()))+from_date_time[-4:-1])
    milliseconds =  int(str(int((datetime.datetime.strptime(string, pattern) - epoch).total_seconds()))+string[-4:-1])
    return milliseconds


# Get lastest available index
def check_index():

    # Get indices list
    indices = []
    list = es.indices.get_aliases()
    for index in list:
        # We only care for 'logstash-*'
        if index[0:9] == 'logstash-':
            indices.append(str(index))
    debug('check_index: found '+str(len(indices))+' indices')
    if indices == []:
        debug('ERROR check_index: No index found! Exiting.')
        sys.exit(1)
    indices = sorted(indices, reverse=True)
    debug('check_index: returning "'+indices[0]+'"')
    return indices[0]


def get_latest_event_timestamp_dummy_load():
    # fake_timestamp = 0

    # Making up latest_event available in ES from current time - 2000ms in the past (rounding the resulting ms to 000)
    now_fake_timestamp = int(datetime.datetime.utcnow().strftime('%s')+'000') - 2000

    # Loop from now-2000 to the beginning of time in reverse to find the highest valid dummy timestamp
    for dummy_timestamp in xrange(now_fake_timestamp, 0, -1):
        hash_sum = 0
        dummy_timestamp_hash = hashlib.md5(str(dummy_timestamp)).hexdigest()

        # for hash_pos in range(0, len(dummy_timestamp_hash) + 1):
        #     if dummy_timestamp_hash[hash_pos - 1:hash_pos].isdigit():
        #         hash_sum += int(dummy_timestamp_hash[hash_pos - 1:hash_pos])
        # print hash_sum

        # Iterate the hash and use only the integers to add them all together (ignoring letters)
        for hash_pos in dummy_timestamp_hash:
            if hash_pos.isdigit():
                hash_sum += int(hash_pos)

        if hash_sum >= dummy_factor:
            break

    # if fake_timestamp == 0:
    #     print "ERROR get_latest_event_timestamp_dummy_load: failed to find a valid timestamps "+str(now_fake_timestamp)+" "+from_epoch_milliseconds_to_string(now_fake_timestamp)
    #     exit(1)

    debug("get_latest_event_timestamp_dummy_load: dummy_timestamp: "+str(dummy_timestamp)+" "+from_epoch_milliseconds_to_string(dummy_timestamp))
    return dummy_timestamp


def get_latest_event_timestamp(index):
    if DEBUG:
        current_time = int(datetime.datetime.utcnow().strftime('%s%f')[:-3])
    # if host_to_search:
    #     res = es.search(size=1, index=index, doc_type=doc_type, fields="@timestamp", sort="@timestamp:desc",
    #                     body={
    #                         "query":
    #                             {"match_phrase": {"host": host_to_search}}
    #                     }
    #                     )
    if host_to_search or value1:
        res = es.search(size=1, index=index, doc_type=doc_type, fields="@timestamp", sort="@timestamp:desc",
                        body=query_latest)
    else:
        res = es.search(size=1, index=index, doc_type=doc_type, fields="@timestamp", sort="@timestamp:desc",
                        body={
                            "query":
                                {"match_all": {}}
                        }
                        )

    debug("get_latest_event_timestamp: "+str(res))

    # At least one event should return, otherwise we have an issue.
    # On To-Do: to go a logstash index back trying to find the last event (it might be midnight...)
    if len(res['hits']['hits']) != 0:
        timestamp = res['hits']['hits'][0]['sort'][0]
        # # Discard milliseconds and round down to seconds
        # timestamp = (timestamp/1000)*1000
        debug("get_latest_event_timestamp: "+str(timestamp)+" "+from_epoch_milliseconds_to_string(timestamp))
        if DEBUG:
            debug("get_latest_event_timestamp: Execution time: " + str(int(datetime.datetime.utcnow().strftime('%s%f')[:-3]) - current_time) + "ms")
        return timestamp
    else:
        print "ERROR: get_latest_event_timestamp: No results found with the current search criteria under index="+index
        print "INFO: Please use --index, --type or --hostname"
        sys.exit(1)


# When we are NOT under -f or --nonstop (dummy_load)
def get_latest_events_dummy_load(latest_event_timestamp):
    # from_date_time_milliseconds = from_string_to_epoch_milliseconds(search_events_dummy_load_from_date_time)
    debug("get_latest_events_dummy_load: latest_event_timestamp: "+str(latest_event_timestamp)+" "+from_epoch_milliseconds_to_string(latest_event_timestamp))

    hits = []
    score = None
    host = 'server-1.example.com'
    path = '/var/log/httpd/access_log'
    message_begin = '192.168.1.1, 192.168.1.2, 192.168.1.3 - - '
    message_end = ' "GET /dummy.html HTTP/1.1" 200 51 0/823 "Agent" "Referer"'
    max_score = None
    shards = {'successful': 5, 'failed': 0, 'total': 5}
    time_out = False

    # hash_sum = 0
    # Loop from provided from_date_time to current time - 2 seconds
    # (to simulate ES indexing time (the newest event will be always 2 seconds in the past)
    # for dummy_timestamp in range(from_date_time_milliseconds, (int(datetime.datetime.utcnow().strftime('%s%f')[:-3])) - 2000 ):

    #
    # for dummy_timestamp in range((int(datetime.datetime.utcnow().strftime('%s%f')[:-3])) - 2000, 1 ):
    for dummy_timestamp in xrange(latest_event_timestamp, 0, -1):
        hash_sum = 0

        # Hash the timestamp from the loop to have a predictive pseudo random sequence:
        # Given the same dummy_factor and the sime epoch time the sequence of timestamp will always be the same
        dummy_timestamp_hash = hashlib.md5(str(dummy_timestamp)).hexdigest()

        # Iterate the hash and use only the integers to add them all together (ignoring letters)
        for hash_pos in dummy_timestamp_hash:
            if hash_pos.isdigit():
                hash_sum += int(hash_pos)

        # Dummy_factor = threshold above which we use the timestamps and discard the rest
        if hash_sum > dummy_factor:
            debug("get_latest_events_dummy_load: dummy_timestamp: "+str(dummy_timestamp)+" hash_sum: "+str(hash_sum))

            doc_id = 'ES_DUMMY_ID_' + str(dummy_timestamp)[-8:]
            fields = {'path': [path],
                      'host': [host],
                      'message': [message_begin + from_epoch_milliseconds_to_string(dummy_timestamp) + message_end],
                      '@timestamp': [from_epoch_milliseconds_to_string(dummy_timestamp)]
                      }
            hit = { 'sort': [dummy_timestamp],
                    '_type': doc_type,
                    '_index': index,
                    '_score': score,
                    'fields': fields,
                    '_id': doc_id
                    }
            hits.append(hit)
        # When in Single Run we just need number of 'docs'. Breaking loop once we have them.
        if len(hits) >= docs:
            break

    debug('get_latest_events_dummy_load: total hits: ' + str(len(hits)))

    hits = {'hits':hits}
    hits['total'] = len(hits)
    hits['max_score'] = max_score

    res = {'hits': hits}
    res['_shards'] = shards
    res['took'] = len(hits) * 10 # Number of events * 10 "dummy" milliseconds
    res['time_out'] = time_out

    # # Let's simulate that ES takes some time to fulfill the request
    # time2.sleep(1.5)

    return res


# When we are NOT under -f or --nonstop
def get_latest_events(index):
    # global event_pool
    # global print_pool
    # to_print = []

    if DEBUG:
        current_time = int(datetime.datetime.utcnow().strftime('%s%f')[:-3])

    if host_to_search or value1:
        res = es.search(size=docs, index=index, doc_type=doc_type, fields="@timestamp,message,path,host",
                        sort="@timestamp:desc",
                        body=query_latest)
    else:
        res = es.search(size=docs, index=index, doc_type=doc_type, fields="@timestamp,message,path,host",
                        sort="@timestamp:desc",
                        body={
                            "query":
                                {"match_all": {}}
                            }
                        )

    debug("get_latest_events: "+str(res))

    # At least one event should return, otherwise we have an issue.
    # On To-Do: to go a logstash index back trying to find the last event (it might be midnight...)
    if len(res['hits']['hits']) != 0:
        timestamp = res['hits']['hits'][0]['sort'][0]

        to_object(res)

        single_run_purge_event_pool(event_pool)

        debug("get_latest_event: timestamp: " + str(timestamp) + " " + from_epoch_milliseconds_to_string(timestamp))
        if DEBUG:
            debug("get_latest_events: execution time: " + str(int(datetime.datetime.utcnow().strftime('%s%f')[:-3]) - current_time) + " ms")
        return timestamp # Needed???
    else:
        print "ERROR: get_latest_events: No results found with the current search criteria under index="+index
        print "INFO: Please use --index, --type or --hostname"
        sys.exit(1)


# Inserts events into event_pool{}
def to_object(res):
    debug("to_object: in: len(event_pool) "+str(len(event_pool)))
    debug("to_object: hits: "+str(len(res['hits']['hits'])))

    for hit in res['hits']['hits']:
        if 'host' in hit['fields']:
            host = str(hit['fields']['host'][0])
        else:
            host = 'None'
        id = str(hit['_id'])
        timestamp = str(hit['sort'][0])
        message = hit['fields']['message'][0]

        # Every new event becomes a new key in the dictionary. Duplicated events (_id) cancel themselves (Only a copy remains)
        # In case an event is retrieved multiple times (same ID) it won't cause duplicates.
        event_pool[id] = { 'timestamp': timestamp, 'host': host,'type': doc_type, 'message': message }

    debug("to_object: out: len(event_pool) "+str(len(event_pool)))
    return


# Get the newest event_timestamp from the event_pool
def get_newest_event_timestamp(get_newest_event_event_pool_copy):
    temp_timestamp = 0

    # No events to play with
    if len(get_newest_event_event_pool_copy) == 0:
        return 0

    for event in get_newest_event_event_pool_copy:
        if int(get_newest_event_event_pool_copy[event]['timestamp']) > temp_timestamp:
            temp_timestamp = int(get_newest_event_event_pool_copy[event]['timestamp'])
    # Error when there are events in the event_pool but we can't find any
    if temp_timestamp  == 0:
        print "ERROR get_newest_event_timestamp: temp_timestamp == 0"
        exit(1)
    else:
        debug('get_newest_event_timestamp: temp_timestamp: '+str(temp_timestamp))
        return temp_timestamp


# Find what's the next available event in the pool given a timestamp, without invading the safeguard (to_the_past)
def get_pointer_ahead(proposed_timestamp):
    debug('get_pointer_ahead: proposed_timestamp: '+str(proposed_timestamp))
    candidates = []
    get_pointer_ahead_event_pool_copy = event_pool.copy()

    # No events to play with
    if len(get_pointer_ahead_event_pool_copy) == 0:
        return 0

    newest_event_timestamp = get_newest_event_timestamp(get_pointer_ahead_event_pool_copy)

    # Something went wrong. The newest event is older than the one we provided.
    if newest_event_timestamp <= proposed_timestamp:
        return 0

    # Loop event_pool searching for timestamps older than the proposed_timestamp and at the same time
    # not in the range between the newest event in the pool and the 10s safeguard (to_the_past)
    # using newest_event_timestamp
    for event in get_pointer_ahead_event_pool_copy:
        event_timestamp = int(get_pointer_ahead_event_pool_copy[event]['timestamp'])
        if event_timestamp == proposed_timestamp:
            print "ERROR get_pointer_ahead: proposed_timestamp found in the event_pool"
            exit(1)
        if event_timestamp > proposed_timestamp and event_timestamp < (newest_event_timestamp - to_the_past):
            candidates.append(event_timestamp)

    debug('get_pointer_ahead: candidates found: '+str(len(candidates)))

    # Sort and choose the smaller number among the candidates
    if candidates:
        candidate = sorted(candidates)[0]
        if candidate > proposed_timestamp:
            debug('get_pointer_ahead: return candidate'+str(candidate))
            return candidate
        else:
            debug('get_pointer_ahead: return no candidate')
            return 0
    else:
        debug('get_pointer_ahead: no candidates')
        return 0


def purge_event_pool(event_pool):
    global pointer
    # pointer_coming_in = pointer
    debug("purge_event_pool: in: "+str(len(event_pool)))
    # debug("purge_event_pool: ten_seconds_ago "+from_epoch_milliseconds_to_string(ten_seconds_ago))
    debug("purge_event_pool: Starting with pointer "+str(pointer)+" "+from_epoch_milliseconds_to_string(pointer))

    to_print = []
    for event in event_pool.copy():
        event_timestamp = int(event_pool[event]['timestamp'])
        #
        # if event_timestamp >= (pointer - interval) and event_timestamp < pointer:
        if event_timestamp >= pointer and event_timestamp < (pointer + interval):
            #
            # I don't understand how this part works :'(
            event_to_print = event_pool[event]
            # Add event ID
            event_to_print['id'] = event
            #
            #
            # Print...
            to_print.append(event_pool[event])
            # delete...
            event_pool.pop(event)
        # elif event_timestamp < (pointer - interval):
        elif event_timestamp < pointer:
            # ...discard what is below last output.
            debug("purge_event_pool: Discarded event @timestamp " + from_epoch_milliseconds_to_string(event_timestamp)+" "+str(event))
            #
            # print "WARNING purge_event_pool: Discarded event with @timestamp "+from_epoch_milliseconds_to_string(event_timestamp)+" "+str(event)
            #
            event_pool.pop(event)

    # Sort by timestamp
    def getKey(item):
        return item['timestamp']

    # Print (add to print_pool) and let wait() function to print it out later
    for event in sorted(to_print,key=getKey):
        if show_headers:
            print_pool.append(from_epoch_milliseconds_to_string(event['timestamp']) + " " + event['id'] + " " + event['host'] + " " + event['type'] + " " + event['message'] + '\n')
        else:
            print_pool.append(event['message'] + '\n')
        # Make the this event the current pointer (they are sorted so the last one will be the newest)
        pointer = int(event['timestamp'])

    # Check whether there is a gap between the new pointer mark and the available events in the pool
    # and in that case move ahead to match it.
    # Usefull when we are falling behind due lack of new events near our current pointer
    proposed_pointer = get_pointer_ahead(pointer)
    debug('purge_event_pool: get_pointer_ahead(pointer): '+str(proposed_pointer))
    if proposed_pointer == 0:
        # No better option. Just move ahead 1 ms.
        debug('purge_event_pool: proposed_pointer: No better option. Pointer += 1')
        pointer += 1
    else:
        # There is a better option ahead
        debug('purge_event_pool: proposed_pointer: '+str(proposed_pointer)+' which is a jump of '+str(proposed_pointer - pointer)+' ms')
        pointer = proposed_pointer

    debug("purge_event_pool: out: "+str(len(event_pool)))
    debug("purge_event_pool: len(print_pool) "+str(len(print_pool)))
    debug("purge_event_pool: Finishing with pointer:"+str(pointer)+" "+from_epoch_milliseconds_to_string(pointer))
    return


def single_run_purge_event_pool(event_pool):
    # global event_pool
    global print_pool
    to_print = []

    for event in event_pool:
        #
        # I don't understand how this part works :'(
        event_to_print = event_pool[event]
        # Add event ID
        event_to_print['id'] = event
        #
        #
        to_print.append(event_pool[event])

    # Sort by timestamp
    def getKey(item):
        return item['timestamp']

    for event in sorted(to_print, key=getKey):
        if show_headers:
            print_pool.append(
                from_epoch_milliseconds_to_string(event['timestamp'])+" "+event['id']+" "+event['host'] + " " + event['type'] + " " + event['message'] + '\n')
        else:
            print_pool.append(event['message'] + '\n')

    # what_to_do_while_we_wait()
    # print_pool = []
    # event_pool = {}


# ES Search simulator for testing purposes (dummy load)
def search_events_dummy_load(search_events_dummy_load_from_date_time):
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

    from_date_time_milliseconds = from_string_to_epoch_milliseconds(search_events_dummy_load_from_date_time)
    debug("search_events_dummy_load: from_date_time: "+str(from_date_time_milliseconds)+" "+search_events_dummy_load_from_date_time)

    hits = []
    score = None
    host = 'server-1.example.com'
    path = '/var/log/httpd/access_log'
    message_begin = '192.168.1.1, 192.168.1.2, 192.168.1.3 - - '
    message_end = ' "GET /dummy.html HTTP/1.1" 200 51 0/823 "Agent" "Referer"'
    max_score = None
    shards = {'successful': 5, 'failed': 0, 'total': 5}
    time_out = False
    # hash_sum = 0

    # Loop from provided from_date_time to current time - 2 seconds
    # (to simulate ES indexing time (the newest event will be always 2 seconds in the past)
    for dummy_timestamp in xrange(from_date_time_milliseconds, (int(datetime.datetime.utcnow().strftime('%s%f')[:-3])) - 2000 ):
        hash_sum = 0

        # Hash the timestamp from the loop to have a predictive pseudo random sequence:
        # Given the same dummy_factor and the sime epoch time the sequence of timestamp will always be the same
        dummy_timestamp_hash = hashlib.md5(str(dummy_timestamp)).hexdigest()

        # # Iterate the hash and use only the integers to add them all together (ignoring letters)
        # for hash_pos in range(0, len(dummy_timestamp_hash) + 1):
        #     if dummy_timestamp_hash[hash_pos - 1:hash_pos].isdigit():
        #         hash_sum += int(dummy_timestamp_hash[hash_pos - 1:hash_pos])

        # Iterate the hash and use only the integers to add them all together (ignoring letters)
        for hash_pos in dummy_timestamp_hash:
            if hash_pos.isdigit():
                hash_sum += int(hash_pos)

        # Dummy_factor = threshold above which we use the timestamps and discard the rest
        if hash_sum > dummy_factor:
            debug("search_events_dummy_load: dummy_timestamp: "+str(dummy_timestamp)+" hash_sum: "+str(hash_sum))

            doc_id = 'ES_DUMMY_ID_' + str(dummy_timestamp)[-8:]
            fields = {'path': [path],
                      'host': [host],
                      'message': [message_begin + from_epoch_milliseconds_to_string(dummy_timestamp) + message_end],
                      '@timestamp': [from_epoch_milliseconds_to_string(dummy_timestamp)]
                      }
            hit = { 'sort': [dummy_timestamp],
                    '_type': doc_type,
                    '_index': index,
                    '_score': score,
                    'fields': fields,
                    '_id': doc_id
                    }
            hits.append(hit)

    debug('search_events_dummy_load: total hits: ' + str(len(hits)))

    hits = {'hits':hits}
    hits['total'] = len(hits)
    hits['max_score'] = max_score

    res = {'hits': hits}
    res['_shards'] = shards
    res['took'] = len(hits) * 10 # Number of events * 10 "dummy" milliseconds
    res['time_out'] = time_out

    # # Let's simulate that ES takes some time to fulfill the request
    # time2.sleep(1.5)

    return res


def search_events(from_date_time):
    if DEBUG:
        current_time = int(datetime.datetime.utcnow().strftime('%s%f')[:-3])
        debug("search_events: from_date_time: "+from_date_time+" "+str(from_string_to_epoch_milliseconds(from_date_time)))
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
    # http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search
    debug("search_events: host: "+host_to_search)

    query_search['query']['filtered']['filter']['range'] = {"@timestamp": {"gte": from_date_time}}

    res = es.search(size="10000", index=index, doc_type=doc_type, fields="@timestamp,message,path,host",
                        sort="@timestamp:asc", body=query_search)

    if DEBUG:
        debug("search_events: Execution time: "+str( int(datetime.datetime.utcnow().strftime('%s%f')[:-3]) - current_time)+"ms" )
    return res


def wait(milliseconds):
    # Current time in Epoch milliseconds
    current_time = int(datetime.datetime.utcnow().strftime('%s%f')[:-3])
    final_time = current_time + milliseconds
    len_print_pool = len(print_pool)

    if len_print_pool == 0:
        debug("wait: len(print_pool) == 0")

    while final_time > current_time:
        current_time = int(datetime.datetime.utcnow().strftime('%s%f')[:-3])
        what_to_do_while_we_wait()
        # # Sleep just a bit to avoid hammering the CPU (to improve)
        # time2.sleep(.01)


def single_run_what_to_do_while_we_wait():
    for i in range(0,len( print_pool ) ):
        sys.stdout.write( print_pool[i] )
        sys.stdout.flush()


def what_to_do_while_we_wait():
    global print_pool
    len_print_pool_2 = len( print_pool )
    # print "RRRRR", len_print_pool_2
    wait_interval = interval + .0

    for i in range(0,len_print_pool_2 ):
        sys.stdout.write( print_pool[i] )
        sys.stdout.flush()

        # Seamless scroll: first try
        time2.sleep( (interval / len_print_pool_2 + .0) / wait_interval )
        # print "RRRR",(interval / len_print_pool_2 + .0) / wait_interval

    print_pool = []


def thread_execution(from_date_time):
    debug("thread_execution: from_date_time: "+str(from_string_to_epoch_milliseconds(from_date_time))+" "+str(from_date_time))

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
        debug("thread_execution: Starting " + self.name)
        thread_execution(self.from_date_time)
        debug("thread_execution: Exiting " + self.name)
        del self


### Main

# Ctrl+C handler
signal.signal(signal.SIGINT, signal_handler)

# --debug
if args.debug:
    DEBUG = args.debug
else:
    DEBUG = None

debug("main: Now: "+str(datetime.datetime.utcnow().strftime('%s%f')[:-3])+" "+from_epoch_milliseconds_to_string(datetime.datetime.utcnow().strftime('%s%f')[:-3]))
debug("main: version 0.9.6")

interval = 1000  # milliseconds - Size of the chunk of time used to move events from the event_pool to print_pool

## { "_id": {"timestamp":"sort(in milliseconds)", "host":"", "type":"", "message":"") }
event_pool = {} # Memory space to store the events retrieved from ES

print_pool = [] # from those, the list of event ready to print on the next output

to_the_past = 10000  # milliseconds - How far we move the pointer to the past to give ES time to consolidate
                     #                Useful when tailing logs from several servers at the same time

dummy_factor = 145 # Probability of finding dummy events when using dummy endpoint (the lower the higher the probability)

# Mutable query object base for main search
query_search = {
    "query": {
        "filtered": {
            "query": {
                "bool": {
                    "must": []
                }
            },
            "filter": {
                "range": {}
            }
        }
    }
}
# and for non continuous search (datetime filter not necessary, we will sort descendent in ES and request only the number of 'docs')
query_latest = {
    "query": {
        "filtered": {
            "query": {
                "bool": {
                    "must": []
                }
            }
        }
    }
}


### Args
# --endpoint
if args.endpoint == 'dummy' or args.endpoint == 'DUMMY':
    DUMMY = True
    print "INFO: Using Dummy Load"
else:
    DUMMY = False
    endpoint = normalize_endpoint(args.endpoint)
# --type
doc_type = args.type
# --host
if args.hostname:
    host_to_search = args.hostname
    query_search['query']['filtered']['query']['bool']['must'].append({"match_phrase": {"host": host_to_search}})
    query_latest['query']['filtered']['query']['bool']['must'].append({"match_phrase": {"host": host_to_search}})
else:
    host_to_search = ''
# --showheaders. Show @timestamp, document ID, hostname and type columns from the output.
if args.showheaders:
    show_headers = True
else:
    show_headers = False
# -f --nonstop
if args.nonstop:
    non_stop = True
else:
    non_stop = False
# -n --docs
docs = int(args.docs)
if docs < 1 or docs > 10000:
    print "ERROR: Document range has to be between 1 and 10000"
    sys.exit(1)
# --level
if args.javalevel:
    value1 = args.javalevel
    query_search['query']['filtered']['query']['bool']['must'].append({"match": {"level": value1}})
    query_latest['query']['filtered']['query']['bool']['must'].append({"match": {"level": value1}})
# --javaclass
elif args.javaclass:
    value1 = args.javaclass
    query_search['query']['filtered']['query']['bool']['must'].append({"match": {"class": value1}})
    query_latest['query']['filtered']['query']['bool']['must'].append({"match": {"class": value1}})
# --httpresponse
elif args.httpresponse:
    value1 = args.httpresponse
    query_search['query']['filtered']['query']['bool']['must'].append({"match": {"server_response": value1}})
    query_latest['query']['filtered']['query']['bool']['must'].append({"match": {"server_response": value1}})
# --method
elif args.httpmethod:
    value1 = args.httpmethod
    query_search['query']['filtered']['query']['bool']['must'].append({"match": {"method": value1}})
    query_latest['query']['filtered']['query']['bool']['must'].append({"match": {"method": value1}})
else:
    value1 = ''

# Workaround to make it work in AWS AMI Linux
# Python in AWS fails to locate the CA to validate the ES SSL endpoint and we need to specify it
# https://access.redhat.com/articles/2039753
if platform.platform()[0:5] == 'Linux':
    ca_certs = '/etc/pki/tls/certs/ca-bundle.crt'
else:
    # On the other side, in OSX works like a charm.
    ca_certs = None

# http://elasticsearch-py.readthedocs.io/en/master/
if not DUMMY:
    es = Elasticsearch([endpoint],verify_certs=True, ca_certs=ca_certs)

if not DUMMY:
    # --index
    if not args.index:
        index = check_index() # Get the latest available index from ES
    else:
        index = args.index
else:
    # When using DUMMY endpoint index = today
    index = datetime.datetime.utcnow().strftime("logstash-%Y.%m.%d")

# When not under -f just get the latest and exit
if non_stop == False:
    debug('Entering Single Run...')
    if not DUMMY:
        get_latest_events(index)
        single_run_what_to_do_while_we_wait()
    else:
        # current_time = from_epoch_milliseconds_to_string( int(datetime.datetime.now().strftime('%s%f')[:-3]) )
        # from_date_time = current_time
        # res = search_events_dummy_load(from_date_time)
        # to_object(res)
        # single_run_purge_event_pool(event_pool)

        latest_event_timestamp = get_latest_event_timestamp_dummy_load()
        # res = search_events_dummy_load( from_epoch_milliseconds_to_string(latest_event_timestamp))
        res = get_latest_events_dummy_load(latest_event_timestamp)
        to_object(res)
        single_run_purge_event_pool(event_pool)
        single_run_what_to_do_while_we_wait()

    debug('Single Run finished. Exiting.')
    sys.exit(0)

# Get the latest event timestamp from the Index
if not DUMMY:
    latest_event_timestamp = get_latest_event_timestamp(index)
else:
    latest_event_timestamp = get_latest_event_timestamp_dummy_load() # No index necessary for dummy_load

# Go 10 seconds to the past. There is where we place "in the past" pointer to give time to ES to consolidate its index.
pointer = latest_event_timestamp - to_the_past

# thread = Threading(1,"Thread-1", ten_seconds_ago)
thread = Threading(1,"Thread-1", pointer)

while True:

    # From timestamp in milliseconds to Elasticsearch format (seconds.milliseconds). i.e: 2016-07-14T13:37:45.123Z
    # from_date_time = from_epoch_milliseconds_to_string(ten_seconds_ago)
    from_date_time = from_epoch_milliseconds_to_string(pointer)

    if not thread.isAlive():
        thread = Threading(1,"Thread-1", from_date_time)
        # thread = Threading(1,"Thread-1", (int(datetime.datetime.utcnow().strftime('%s%f')[:-3])) - to_the_past )
        thread.start()

    # "Send to print" and purge oldest events in the pool
    purge_event_pool(event_pool)

    # Wait for Elasticsearch to index a bit more of stuff (and Print whatever is ready meanwhile)
    wait(interval)

    difference = get_newest_event_timestamp(event_pool) - pointer
    print "DDDD", pointer, difference

    # And here we go again...
