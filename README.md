```shell
yum install python3

pip3 install pyspark

export PYSPARK_PYTHON=python3

pyspark --packages io.delta:delta-core_2.11:0.4.0
```

Load a file of vertical column names and retrieves a schema with string type<br>
<sub><sup>([:-1] is a dirty hack to remove the last added coma)</sub></sup>

```shell
schema = open('MSC.columns', 'r').read().replace('\n', ' STRING,')[:-1]
```
