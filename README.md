# es-cli
[![CircleCI](https://circleci.com/gh/rerost/es-cli/tree/master.svg?style=svg&circle-token=df496b759fd684d97bf6f94c9251763960fcc049)](https://circleci.com/gh/rerost/es-cli/tree/master)

This tool is **still under development**.

It's high useage elasticsearch's operation wrapper tool.
For example, creating index and add/remove alias operations is frequently performed, but JSON API is too complicated(I can not remember).
So I create this tool

## Term
I said `details` is for `mappings`, `settings`, and `aliases`.
Its for creating indecies.
https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html

## Installation
`go get -u github.com/rerost/es-cli`

## Usage
### Format
```
$ es-cli <operation> <target> args...
$ es-cli [--host=HOST] [--user=BASIC_AUTH_USER] [--pass=BASIC_AUTH_PASSWORD] [--type=ELASTICSEARCH_DOCUMENT_TYPE] <operation> <target> args...
```

### Index API
```
$ es-cli list index
$ es-cli create index <index_name> <detail_json_file>
$ es-cli create index <index_name> # Read detail json by stdin
$ es-cli copy index <src_index_name> <dst_index_name>
$ es-cli count index <index_name> # Return total count of documents
$ es-cli delete index <index_name>
$ es-cli dump index <index_name> # Dump details & docs
$ es-cli restore index <dumped_file> # Insert docs from dumped doc file(Without details)
$ es-cli restore index # Insert docs from dumped doc file(Without details)
```


### Detail API
```
$ es-cli get detail <index_name> # Get settings, alias, mappings for creat index
$ es-cli update detail <alias_name> <detail_json_file> # Zero downtime(without write) update detail
$ es-cli update detail <alias_name> # Read detail json by stdin
```

### Alias API
```
$ es-cli add alias <alias_name> <index_name1> <index_name2> ...
$ es-cli remove alias <alias_name> <index_name1> <index_name2> ...
$ es-cli list alias <alias_name>
```

### Task API
```
$ es-cli get task <task_id>
$ es-cli list task
```

### Experimental API
```
$ es-cli copy remote remoteHost copyIndexName user pass (type or not)
```

## Configuration
You can use configuration file.
es-cli see options order by command options > current directory > home direcotry.

configrutaion file name is "`escli.json`
the configuration file's format is json.
e.g
```
{
  "user": "user", 
  "pass": "pass",
  "host": "http://localhost:9200",
  "type": "_doc"
}
```
