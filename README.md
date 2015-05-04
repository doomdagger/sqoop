Sqoop
============

For Complete Document, see [Apache Sqoop Java Client API](http://sqoop.apache.org/docs/1.99.4/ClientAPI.html)

### What is it

This project provides two functionality for sqoop users, including:
* scanner
* worker

> **Scanner** will transfer a raw-type configuration file to a ready-to-use one by scanning a required source file.
Util now, we only support source file in `csv` format: `TABLE_NAME,PRIME_KEY_NAME`. Click [here](http://gitlab.yixinonline.org/heli11/sqoop/blob/master/test.csv) to have a look at
our sample `csv` file.

> **Worker** will parse a ready-to-use configuration file into link configs and job configs, and then communication with
sqoop server to execute appropriate commands, such as creating or updating links, jobs, etc.


### Usage

The built executable jar file includes the following options:
```
Usage: <main class> [options] 
  Options:
  * --action, -a
       the action to take:
		worker - use the specified configuration file to
       communicate with sqoop server.
		scanner - use the specified configuration file to
       generate ready-to-use configuration file for worker action.
  * --configuration, -c
       the path of the configuration file, used by worker action or scanner
       action
    --help, -h
       display help messages
       Default: false
    --input, -i
       the path of the input file, only when scanner action is taken, this
       parameter is *required*
    --output, -o
       the path of the output file, omit this parameter will output will be
       directed to console
```

You can use `--help` or `-h` option to show the message above:
```sh
# java -jar BUILT_JAR_FILE_PATH --help
java -jar sqoop-worker-1.0-SNAPSHOT.jar --help
```

Using full functionality of **Scanner**:
```sh
# java -jar BUILT_JAR_FILE_PATH --action scanner --configuration RAW_CONF_FILE_PATH 
#                               --input INPUT_FILE_PATH --output OUTPUT_FILE_PATH 
java -jar sqoop-worker-1.0-SNAPSHOT.jar -a scanner -c raw.conf -i test.csv -o ready.conf
```

Using full functionality of **Worker**:
```bash
# java -jar BUILT_JAR_FILE_PATH --action worker --configuration CONF_FILE_PATH 
java -jar sqoop-worker-1.0-SNAPSHOT.jar -a worker -c ready.conf
```

### Demo raw.conf File
```conf
# the packing process will proceed sequentially.
# Please arrange your definitions of links and jobs into groups

# link #1
linkConfig.cid=2
linkConfig.name=Vampire
linkConfig.creationUser=LiHe
linkConfig.connectionString=jdbc:mysql://localhost/my
linkConfig.jdbcDriver=com.mysql.jdbc.Driver
linkConfig.username=root
linkConfig.password=root

# link #2
linkConfig.cid=1
linkConfig.name=JOKE
linkConfig.uri=hdfs://nameservice1:8020/

# job #1
jobConfig.fromLinkName=Vampire
jobConfig.toLinkName=JOKE
jobConfig.name=oracle-{fromJobConfig.tableName}
jobConfig.creationUser=LiHe
fromJobConfig.schemaName=sqoop
fromJobConfig.tableName={fromJobConfig.tableName}
fromJobConfig.partitionColumn={fromJobConfig.partitionColumn}
fromJobConfig.sql=select * from {fromJobConfig.tableName}
toJobConfig.outputDirectory=/usr/tmp/{fromJobConfig.tableName}/[yyyy-MM-dd]
throttlingConfig.numExtractors=3
throttlingConfig.numLoaders=3
```