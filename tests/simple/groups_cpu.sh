#!/usr/bin/env bash

$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --num-executors=1 --executor-cores=2 --executor-memory=8G \
    --deploy-mode cluster \
    --name groups.py_CPU \
    --conf spark.sql.extensions=org.apache.spark.sql.OapExtensions \
    --conf spark.executorEnv.LD_LIBRARY_PATH=$HOME/miniconda2/envs/oapenv/lib \
    --conf spark.executor.extraLibraryPath=$HOME/miniconda2/envs/oapenv/lib \
    --conf spark.driver.extraLibraryPath=$HOME/miniconda2/envs/oapenv/lib \
    --conf spark.executor.extraClassPath=$HOME/miniconda2/envs/oapenv/oap_jars/plasma-sql-ds-cache-1.2.0-with-spark-3.1.1.jar:$HOME/miniconda2/envs/oapenv/oap_jars/pmem-common-1.2.0-with-spark-3.1.1.jar \
    --conf spark.driver.extraClassPath=$HOME/miniconda2/envs/oapenv/oap_jars/plasma-sql-ds-cache-1.2.0-with-spark-3.1.1.jar:$HOME/miniconda2/envs/oapenv/oap_jars/pmem-common-1.2.0-with-spark-3.1.1.jar \
    --conf spark.sql.columnVector.offheap.enabled=false \
    --conf spark.memory.offHeap.enabled=false \
    --conf spark.oap.cache.strategy=guava \
    --conf spark.sql.oap.cache.memory.manager=offheap \
    --conf spark.executor.memoryOverhead=10g \
    --conf spark.executor.sql.oap.cache.offheap.memory.size=10g \
    --conf spark.sql.oap.parquet.binary.cache.enabled=false \
    --conf spark.sql.oap.parquet.data.cache.enabled=true \
    --conf spark.sql.oap.orc.binary.cache.enabled=true \
    --conf "spark.executor.extraJavaOptions=-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=8090 -Dcom.sun.management.jmxremote.rmi.port=8090 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=localhost" --conf spark.metrics.conf=/home/saudet/aurora4spark/tests/simple/metrics.properties \
    groups.py
