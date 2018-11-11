# es-cli
[![CircleCI](https://circleci.com/gh/rerost/es-cli/tree/master.svg?style=svg&circle-token=df496b759fd684d97bf6f94c9251763960fcc049)](https://circleci.com/gh/rerost/es-cli/tree/master)

This tool is **still under development**
It's only support operations. e.g. create index, reindex, update mapping, ...

## Installation
`go get -u github.com/rerost/es-cli`

## Usage
### Format
```
$ es-cli <operation> <target> args...
$ es-cli [--host=HOST] [--port=PORT] [--user=BASIC_AUTH_USER] [--pass=BASIC_AUTH_PASSWORD] [--type=ELASTICSEARCH_DOCUMENT_TYPE] <operation> <target> args...
```

### Index API
```
$ es-cli list index
$ es-cli create index <index_name> <mapping_json>
$ es-cli create index <index_name> # Write mapping json by stdin
$ es-cli copy index <src_index_name> <dst_index_name>
$ es-cli count index <index_name> # Return total count of documents
$ es-cli delete index <index_name>
```

### Mapping API
```
$ es-cli get mapping <index_name or alias_name>
$ es-cli update mapping <alias_name> # Zero downtime(without write) update mapping
```

### Alias API
```
$ es-cli add alias <alias_name> <index_name1> <index_name2> ...
$ es-cli remove alias <alias_name> <index_name1> <index_name2> ...
```

### Task API
```
$ es-cli get task <task_id>
$ es-cli list task
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
  "host": "http://localhost",
  "port": "9200",
  "type": "_doc"
}
```
