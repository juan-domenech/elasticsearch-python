from argparse import ArgumentParser
import re
import signal  # Dealing with Ctrl+C
import sys
import platform

try:
    from elasticsearch import Elasticsearch
except:
    print "ERROR: elasticsearch module not installed. Please run 'sudo pip install elasticsearch'."
    sys.exit(1)

# Arguments parsing
parser = ArgumentParser(description='Unix like tail command for Elastisearch and Logstash.')
parser.add_argument('-e', '--endpoint', help='ES endpoint URL.', required=True)

parser.add_argument('-d', '--debug', help='Debug', action="store_true")
args = parser.parse_args()


def debug(message):
    if DEBUG:
        print "DEBUG " + str(message)


# Ctrl+C
def signal_handler(signal, frame):
    debug('Ctrl+C pressed!')
    sys.exit(0)


def normalize_endpoint(endpoint):
    end_with_number = re.compile(":\d+$")

    if endpoint[-1:] == '/':
        endpoint = endpoint[:-1]

    if endpoint[0:5] == "http:" and not end_with_number.search(endpoint):
        endpoint = endpoint + ":80"
        return endpoint

    if endpoint[0:6] == "https:" and not end_with_number.search(endpoint):
        endpoint = endpoint + ":443"
        return endpoint

    if not end_with_number.search(endpoint):
        endpoint = endpoint + ":80"
        return endpoint

    return endpoint


### Main

# Ctrl+C handler
signal.signal(signal.SIGINT, signal_handler)

# --debug
if args.debug:
    DEBUG = args.debug
else:
    DEBUG = None

### Args
# --endpoint
if args.endpoint:
    endpoint = normalize_endpoint(args.endpoint)
else:
    exit(1)


# Workaround to make it work in AWS AMI Linux
# Python in AWS fails to locate the CA to validate the ES SSL endpoint and we need to specify it
# https://access.redhat.com/articles/2039753
if platform.platform()[0:5] == 'Linux':
    ca_certs = '/etc/pki/tls/certs/ca-bundle.crt'
else:
    # On the other side, in OSX works like a charm.
    ca_certs = None


es = Elasticsearch([endpoint], verify_certs=True, ca_certs=ca_certs)

# stats = es.cluster.stats()
#
# debug(stats)
#
# cluster_name = stats['cluster_name']
# print "cluster_name:", cluster_name
#
# number_nodes = stats['nodes']['count']['total']
# print number_nodes

# jvm_heap_used_in_bytes = stats['nodes']['jvm']['mem']['heap_used_in_bytes']


# http://elasticsearch-py.readthedocs.io/en/master/api.html#nodes
stats = es.nodes.stats()

# debug(stats)

cluster_name = stats['cluster_name']
debug("cluster_name: "+cluster_name)

# number_nodes = len(stats['nodes'])
# print number_nodes

node_list = []
for node in stats['nodes']:
    # Substitute spaces by -
    node_name = stats['nodes'][node]['name'].replace(" ","-")
    node_list.append(node+":"+node_name)

debug(node_list)

for node in stats['nodes']:

    node_name = node+":"+stats['nodes'][node]['name'].replace(" ","-")

    non_heap_used_in_bytes = stats['nodes'][node]['jvm']['mem']['non_heap_used_in_bytes']
    debug(node_name+" non_heap_used_in_bytes: "+str(non_heap_used_in_bytes))

    heap_used_in_bytes = stats['nodes'][node]['jvm']['mem']['heap_used_in_bytes']
    debug(node_name + " heap_used_in_bytes: " + str(heap_used_in_bytes))

    heap_used_percent = stats['nodes'][node]['jvm']['mem']['heap_used_percent']
    debug(node_name + " heap_used_percent: " + str(heap_used_percent))

    pool_old_used_in_bytes = stats['nodes'][node]['jvm']['mem']['pools']['old']['used_in_bytes']
    debug(node_name + " pool_old_used_in_bytes: " + str(pool_old_used_in_bytes))

    pool_young_used_in_bytes = stats['nodes'][node]['jvm']['mem']['pools']['young']['used_in_bytes']
    debug(node_name + " pool_young_used_in_bytes: " + str(pool_young_used_in_bytes))

    pool_survivor_used_in_bytes = stats['nodes'][node]['jvm']['mem']['pools']['survivor']['used_in_bytes']
    debug(node_name + " pool_survivor_used_in_bytes: " + str(pool_survivor_used_in_bytes))
