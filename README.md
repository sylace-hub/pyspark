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
```
#### load MSC Dataframe

```python
df_msc = spark.read.option('delimiter',';').schema(schema).csv('/data/msc/HUA_DWH-081019-200000.csv')

calling_NB = df_msc.select('CALLINGNUMBER')

CNB_NN = calling_NB.where(calling_NB.CALLINGNUMBER.isNotNull())
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
