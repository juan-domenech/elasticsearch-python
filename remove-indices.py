import time
from argparse import ArgumentParser
from elasticsearch import Elasticsearch
import sys
import platform

#
# Delete old indices in order to get free space in the ES cluster based on the amount of free space we need.
#

# Examples:
# Non-Secure connection
# python remove-indices.py --endpoint elak.example.com:80
# Secure connection
# python remove-indices.py --endpoint https://elak.example.com
# Standard Elasticsearch connection
# python remove-indices.py --endpoint elak.example.com:9200

# In case of error:
# "elasticsearch.exceptions.ConnectionError: ConnectionError(('Connection failed.', CannotSendRequest())) caused by: ConnectionError(('Connection failed.', CannotSendRequest()))"
# Update pip install --upgrade urllib3
# or
# Use a non HTTPS URL

# To-Do:
# Check Endpoint URL to begin with 'htt?://' and end with ':port_num' and change/warn accordingly

# Arguments parsing
parser = ArgumentParser(description='Delete old indices in order to get free space in the ES cluster based on the amount of free space we need.')
parser.add_argument('-e', '--endpoint', help='ES endpoint URL', required=True)
parser.add_argument('-d', '--desired', help='Minimum desired free space percentage(%): 20, 30, etc. Default: 30', default=30)
parser.add_argument('-i', '--indices', help='Minimum number of indices required. Warning! Lowering this may delete the whole cluster. Default: 7', default=7)
parser.add_argument('-p', '--prefix', help='Index prefix to delete. Default: logstash', default='logstash')
parser.add_argument('-r', '--dryrun', help='Run without deleting anything', action="store_true")
args = parser.parse_args()

# Elasticsearch endpoint
endpoint = args.endpoint
print "INFO: endpoint:",endpoint
# Percentage
desired_free_space = int(args.desired)
# Fail safe. It is weird to need space with only a week of logs... let's put a limit on what we can delete.
minimum_indices = int(args.indices)
# Dry Run
if args.dryrun:
    dry_run = True
    print "INFO: *** Running in Dry Run mode ***"
else:
    dry_run = False
# Index prefix
prefix = args.prefix

# Workaround to make it work in AWS AMI Linux
# Python in AWS fails to locate the CA to validate the ES endpoint SSL and we need to specify it :(
# https://access.redhat.com/articles/2039753
if platform.platform()[0:5] == 'Linux':
    ca_certs = '/etc/pki/tls/certs/ca-bundle.crt'
else:
    # On the other side, in OSX works like a charm.
    ca_certs = None

def get_free_space_percentage():
    # http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.client.ClusterClient.stats
    stats = es.cluster.stats()
    total_in_bytes = stats['nodes']['fs']['total_in_bytes']
    free_in_bytes = stats['nodes']['fs']['free_in_bytes']
    print "INFO: total_in_bytes =",total_in_bytes
    print "INFO: free_in_bytes =",free_in_bytes
    free = (free_in_bytes * 100) / total_in_bytes
    return free


def get_indices_list():
    indices = []
    list = es.indices.get_alias()
    for index in list:
        # Use only indices that match our prefix
        if index[0:len(prefix)] == prefix:
            indices.append(str(index))
    return indices


def remove_index(index):
    if not index:
        print "ERROR: Input is empty. Exiting!"
        exit(1)
    output = es.indices.delete(index)
    print "INFO: Index '"+index+"' removed."
    return output


def wait(seconds):
    for secs in range(0,seconds):
        sys.stdout.write('.')
        sys.stdout.flush()
        time.sleep(1)
    print


# http://elasticsearch-py.readthedocs.io/en/master/
# http://charlesleifer.com/blog/setting-up-elasticsearch-with-basic-auth-and-ssl-for-use-with-python/
es = Elasticsearch([endpoint],verify_certs=True, ca_certs=ca_certs)
print "INFO: Connecting to:",endpoint
cluster_name = es.cluster.stats()
print "INFO: Connected to ES cluster:",cluster_name['cluster_name']

free = get_free_space_percentage()

print "INFO: Minimum indices required =", minimum_indices
print "INFO: Current free space =", free,"%"
print "INFO: Desired =",desired_free_space,"%"

if free >= desired_free_space:
    print "INFO: Enough space. Nothing to do."
    exit(0)
else:

    print "WARNING: Clean up needed!"
    indices = get_indices_list()
    indices = sorted(indices)
    print "INFO: Total",len(indices),"indices found."
    print "INFO: From",indices[0],"to",indices[-1]

    # Fail safe. Don't go too far.
    if len(indices) < minimum_indices:
        print "ERROR: Something is wrong. Too few indices. Exiting!"
        exit(1)

    # Loop over all indices and see what we can delete (older first)
    for to_remove in range(0,len(indices)):
        if to_remove >= (len(indices) - minimum_indices):
            print "ERROR: We can't go over the limit of",minimum_indices,"indices. Exiting!"
            exit(1)

        print "INFO: Removing Index '"+indices[to_remove]+"'..."
        if dry_run:
            print "INFO: *** Nothing to remove: We are in Dry Run mode. Skipping! ***"
        else:
            output = remove_index(indices[to_remove])
            # print "DEBUG: ",output
            if not 'acknowledged' in output or output['acknowledged'] != True:
                print "ERROR: Removing operation failed with error ",output
                exit(1)
            wait(60)

        # Check whether we have more space available after deleting one
        free = get_free_space_percentage()
        print "INFO: Free space after delete =",free,"%"
        if free >= desired_free_space:
            print "INFO: Enough space now ("+str(free)+"%). We are done."
            exit(0)

        print "INFO: It is not enough. Deleting more..."

    print "ERROR: We shouldn't be here. Exiting!"
    exit(1)
