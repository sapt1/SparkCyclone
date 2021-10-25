# aurora4spark

- For the plug-in development: [SBT.md](SBT.md).
- For the JavaCPP layer around AVEO, see: [https://github.com/bytedeco/javacpp-presets/tree/aurora/veoffload](https://github.com/bytedeco/javacpp-presets/tree/aurora/veoffload).

## Usage of the plugin

```

$ /opt/spark/bin/spark-submit \
    --name PairwiseAddExample \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-add-pairwise.py

$ /opt/spark/bin/spark-submit \
    --name AveragingExample \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-avg.py

$ /opt/spark/bin/spark-submit \
    --name SumExample \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-sum.py

$ /opt/spark/bin/spark-submit \
    --name SumMultipleColumnsExample \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-sum-multiple.py


$ /opt/spark/bin/spark-submit \
    --name AveragingMultipleColumns5Example \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-avg-multiple.py

$ /opt/spark/bin/spark-submit \
    --name MultipleOperationsExample \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-multiple-operations.py

```

## NCC arguments

A good set of NCC defaults is set up, however if further overriding is needed, it can be done with the following Spark
config:

```
--conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc
--conf spark.com.nec.spark.ncc.debug=true
--conf spark.com.nec.spark.ncc.o=3
--conf spark.com.nec.spark.ncc.openmp=false
--conf spark.com.nec.spark.ncc.extra-argument.0=-X
--conf spark.com.nec.spark.ncc.extra-argument.1=-Y
```

For safety, if an argument key is not recognized, it will fail to launch.

To use the native CSV parser (default is 'off'):

```
--conf spark.com.nec.native-csv=x86
--conf spark.com.nec.native-csv=VE
```

To skip using IPC for parsing CSV:

```
--conf spark.com.nec.native-csv-ipc=false
```

To use String allocation as opposed to ByteArray optimization in `NativeCsvExec`, use:

```
--conf spark.com.nec.native-csv-skip-strings=false
```

## Clustering / resource support

A variety of options are available - not tested with YARN yet.

```
# for Driver, a VE is not needed (at least not yet)
# this is definitely needed
--conf spark.executor.resource.ve.amount=1

# not clear if this is needed
--conf spark.task.resource.ve.amount=1

## This seems to be necessary for cluster-local mode
--conf spark.worker.resource.ve.amount=1

# detecting resources automatically
--conf spark.resources.discoveryPlugin=com.nec.ve.DiscoverVectorEnginesPlugin

# specifying resources via file
--conf spark.executor.resource.ve.discoveryScript=/opt/spark/getVEsResources.sh
```

## Compilation lifecycle

The aurora4spark plugin will translate your Spark SQL queries into a C++ kernel to execute them on the Vector Engine.  
Compilation can take anywhere from a few seconds to a couple minutes.  While insignificant if your queries take hours
you can optimize the compilation time by specifying a directory to cache kernels using the following config.

```
--conf spark.com.nec.spark.kernel.directory=/path/to/compilation/dir
```

If a suitable kernel exists in the directory, the aurora4spark plugin will use it and not compile a new one from
scratch.

### Use a precompiled directory

You can also disable on-demand compilation by specifying a precompiled directory instead.

```
--conf spark.com.nec.spark.kernel.precompiled=/path/to/precompiled/dir
```

If neither are specified, then a random temporary directory will be used (not removed, however).

## Batching

This is to batch ColumnarBatch together, to allow for larger input sizes into the VE. This may however use more on-heap
and off-heap memory.

```
--conf spark.com.nec.spark.batch-batches=3
```

Default is 0, which is just to pass ColumnarBatch directly in.

## Pre-shuffling/hashing

This will try to pre-shuffle the data so that we only need to call the VE in one stage for aggregations. It might be
more performant due to avoiding a coalesce/shuffle afterwards.

```
--conf spark.com.nec.spark.preshuffle-partitions=8
```

##Sorting on Ve
By default all sorting is done on CPU, however there exists possibility to enable sorting on VE. 
--conf spark.com.nec.spark.sort-on-ve=true