# Elasticsearch Python Tools

## Elasticsearch Tail Command `elasticsearch-tail.py`

Humble implementation of a Unix like tail for Elasticsearch. Tested with Logstash indexed content.

### Usage

The only mandatory parameter is `--endpoint`.

Example:
`python elasticsearch-tail.py --endpoint http://elak.example.com`

By default last 10 lines of log are displayed. You can change this behaviour with `--docs` or `-n` switch.

Example:

`python elasticsearch-tail.py --endpoint http://elak.example.com -n 50`

By default ES Type = `apache` is used. You can select other types with `--type`.

Examples:

`python elasticsearch-tail.py --endpoint http://elak.example.com --type java`

`python elasticsearch-tail.py --endpoint http://elak.example.com --type apache`

By default the latest Logstash Index available is used. Optionally you can specify the desired index name.

Example:

`python elasticsearch-tail.py --endpoint http://elak.example.com --index logstash-2016.08.08`

When using `--type java` there are two other selectors available: `--javalevel` and `--javaclass`

Examples:

`python elasticsearch-tail.py --endpoint http://elak.example.com --type java --javalevel ERROR`

`python elasticsearch-tail.py --endpoint http://elak.example.com --type java --javaclas error.handler.java.class`

When using `--type apache` there are two other selectors available: `--httpresponse` and `--httpmethod`

Examples:

`python elasticsearch-tail.py --endpoint http://elak.example.com --type apache --httpresponse 404`

`python elasticsearch-tail.py --endpoint http://elak.example.com --type apache --httpmethod POST`

To have continuous output use `-f` or `--nonstop`.

Example:

`python elasticsearch-tail.py --endpoint http://elak.example.com -f`

To display the native ES timestamp of each event use `--showheaders`.

Example:

`python elasticsearch-tail.py --endpoint http://elak.example.com --showheaders`

To display events belonging to a particular host and ignore the rest use `--hostname`.

Example:

`python elasticsearch-tail.py --endpoint http://elak.example.com --hostname server1.example.com`



## Delete Elasticsearch indices based on the desired free space `remove-indices.py`

**Warning: This script will delete Elasticsearch documents.**

Drop old ES indices using the percentage of desired free space we want in the ES cluster. By default this will delete indices until reach 30% free space.

Example:

`python remove-indices.py --endpoint http://elak.removing.indices.example.com`

This percentage can be changed using `--desired`. Example, this will free up to 10% of free space:

`python remove-indices.py --endpoint http://elak.removing.indices.example.com --desired 10`

This script has a fail-safe of 7 indices. It will stop deleting when only 7 indices remain (no matter the amount of free space in the cluster). This limit can be overriden usin `--indices`.

Example. This will try to get 30% free space (default value) and will delete indices until 6 indices remain (if necessary):

`python remove-indices.py --endpoint http://elak.removing.indices.example.com --indices 6`

For testing purposed the option `--dryrun` is available.



## Remove 'grokparsefailure' Documents `remove_grokparsefailure_documents.py`

Delete all documents in Elasticsearch with the Tag =  `grokparsefailure`.

**Warning: This script will delete Elasticsearch documents.**

Mandatory parameter is `--endpoint`.

Example:

`python remove_grokparsefailure_documents.py --endpoint http://elak.with.grokparsefailure.docs.example.com`
