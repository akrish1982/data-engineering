Install Java 8
Post installation, set JAVA_HOME and PATH variable.

```
JAVA_HOME = C:\Program Files\Java\jdk1.8.0_201
PATH = %PATH%;C:\Program Files\Java\jdk1.8.0_201\bin
```

Download & Install Apache Spark
```
SPARK_HOME  = downloadlocation\spark-3.5.0-bin-hadoop3
HADOOP_HOME = downloadlocation\spark-3.5.0-bin-hadoop3
PATH=%PATH%;downloadlocation\spark-3.0.5-bin-hadoop3\bin
```

to start spark history server:

`
$SPARK_HOME/sbin/start-history-server.sh
`
