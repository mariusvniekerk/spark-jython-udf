[![Build Status](https://travis-ci.org/mariusvniekerk/spark-jython-udf.svg?branch=master)](https://travis-ci.org/mariusvniekerk/spark-jython-udf)

# spark-jython-udf

This is an initial attempt at a spark package that captures the core concepts from
[SPARK-15369](https://github.com/apache/spark/pull/13571) and attempts to turn that into 
an installable spark-package.

## Thanks

This would not have been possible without the considerable efforts of @holdenk pushing 
python forward in the Apache Spark community


## Usage

In a python instance that already has a spark context instantiated and the spark-jython library loaded.

```python
import spark_jython
from pyspark.sql.types import *

def jythonfn(arg1):
    return arg1.split(" ")

returnUDFType = ArrayType(StringType())
jythonUDF = spark_session.catalog.registerJythonFunction("name", function, returnUDFType)

```