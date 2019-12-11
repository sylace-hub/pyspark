#### Using pyspark with delta



```shell
yum install python3

pip3 install pyspark

export PYSPARK_PYTHON=python3

pyspark --packages io.delta:delta-core_2.11:0.4.0
```

#### schema from file

- Load a file of vertical column names 
- Retrieves a schema with string type<br>
<sub><sup>([:-1] is a dirty hack to remove the last added coma)</sub></sup>

```python
schema = open('/data/msc/MSC.columns', 'r').read().replace('\n', ' STRING,')[:-1]

#write to schema file
open("MSC.schema", "w").write(open('MSC.columns', 'r').read().replace('\n', ' String,')[:-1])

schema = open('/data/msc/MSC.schema', 'r').read()
schema = open('/data/output/TRANSACTION.schema', 'r').read()
```
#### Load MSC Dataframe

```python
df_msc = spark.read.option('delimiter',';').schema(schema).csv('/data/msc/HUA_DWH-081019-200000.csv')

calling_NB = df_msc.select('CALLINGNUMBER')

CNB_NN = calling_NB.where(calling_NB.CALLINGNUMBER.isNotNull())

```
#### Using repartition to write to a single file

```python
NumDF.where(NumDF.A_NUMBER.isNull()).repartition(1).write.csv('/data/output/A_NUMBER_NULL.csv')
```

#### Using pyspark with bpython

```python
from pyspark import SparkContext
sc = SparkContext("local", "app")
from pyspark.sql import SQLContext
spark = SQLContext(sc)
```

#### Run a file from Py Shell

```python
run = lambda filename : exec(open(filename).read())
run('filename.py')
```

#### add a custom module to python

```python
>>> import sys
>>> print(sys.path)
['', '/usr/local/bin', '/usr/lib64/python36.zip', '/usr/lib64/python3.6', '/usr/lib64/python3.6/lib-dynload', '/usr/lo
cal/lib64/python3.6/site-packages', '/usr/local/lib/python3.6/site-packages', '/usr/lib64/python3.6/site-packages', '/
usr/lib/python3.6/site-packages']
```

```shell
[root@ingest:/code/lib/py]# vim hello.py

[root@ingest:/usr/local/lib/python3.6/site-packages]# vim my-paths.pth
/code/lib/py
```

#### bpython history

```shell
~/.pythonhist
```

#### execute a script from python shell 
(import * is the trick to import variables inline, in the same scope as the interpreter)

```python
from spark_etl_name import *
```

#### Display Local Variables in Python Shell
```python
for item in list(locals()): print(item)
```

#### Capitalize Dataframe Columns
```python
DF = DF.select([F.col(i).alias(i.upper()) for i in DF.columns])
```
