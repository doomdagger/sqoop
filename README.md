Sqoop Worker
============

For Complete Document, see [Apache Sqoop Java Client API](http://sqoop.apache.org/docs/1.99.4/ClientAPI.html)

### Usage

```bash
# java -jar BUILT_JAR_FILE_PATH CONF_FILE_PATH
java -jar sqoop-worker-1.0-SNAPSHOT.jar somefile.conf
```

### Demo *.conf File
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
jobConfig.name=Vampire
jobConfig.creationUser=LiHe
fromJobConfig.schemaName=sqoop
fromJobConfig.tableName=sqoop
fromJobConfig.partitionColumn=id
fromJobConfig.sql=select * from sqoop
fromJobConfig.columns=id name
fromJobConfig.stageTableName=sqoop
toJobConfig.outputDirectory=/usr/tmp/{yyyy-MM-dd}
throttlingConfig.numExtractors=3
throttlingConfig.numLoaders=3
```