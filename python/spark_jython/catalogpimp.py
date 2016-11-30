from __future__ import absolute_import
from .functions import UserDefinedJythonFunction

from pyspark.sql.catalog import Catalog
from pyspark.sql.types import DataType, StringType


def registerJythonFunction(self, name, fn, returnType=StringType()):
    """
    Register a function to be executed using Jython on the workers.
    The function passed in must either be a string containing your python lambda expression,
    or if you have dill installed on the driver a lambda dill can extract the source for.
    Note that not all Python code will execute in Jython and not all Python
    code will execute well in Jython. However, for some UDFs, executing in Jython
    may be faster as we can avoid copying the data from the JVM to the Python
    executor.

    This is a very experimental feature, and may be removed in future versions
    once we figure out if it is a good idea or not.
    ...Note: Experimental

    :param name: name of the UDF
    :param f: String containing python lambda or python function
    :param returnType: a :class:`DataType` object
    """
    udf = UserDefinedJythonFunction(fn, returnType, name)
    jython = self._jsparkSession.jvm.org.apache.spark.sql.Jython

    pimpedRegistrar = jython.JythonUDFRegistration(self._jsparkSession.udf())

    pimpedRegistrar.registerJython(name, udf._judf)
    return udf


# Monkey patch in this function
Catalog.registerJythonFunction = registerJythonFunction
