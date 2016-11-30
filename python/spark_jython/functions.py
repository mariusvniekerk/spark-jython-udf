from __future__ import absolute_import
import types
from base64 import b64encode
import sys
import types
from io import StringIO
from pyspark import since, SparkContext, cloudpickle
from pyspark.serializers import CloudPickleSerializer
from pyspark.sql.functions import _to_seq, _to_seq, _to_java_column
from pyspark.sql.column import Column
from six import string_types


def _wrap_jython_func(sc, src, ser_vars, ser_imports, setup_code):
    return sc._jvm.org.apache.spark.sql.jython.JythonFunction(
            src, ser_vars, ser_imports, setup_code, sc._jsc.sc())


class UserDefinedJythonFunction(object):
    """
    User defined function in Jython - note this might be a bad idea to use.
    .. versionadded:: 2.0
    .. Note: Experimental
    """
    def __init__(self, func, returnType, name=None, setupCode=""):
        self.func = func
        self.returnType = returnType
        self.setupCode = setupCode
        self._judf = self._create_judf(name)

    def _create_judf(self, name):
        func = self.func
        from pyspark.sql import SQLContext
        sc = SparkContext.getOrCreate()
        # Empty strings allow the Scala code to recognize no data and skip adding the Jython
        # code to handle vars or imports if not needed.
        serialized_vars = ""
        serialized_imports = ""
        if isinstance(func, string_types):
            src = func
        else:
            try:
                import dill
            except ImportError:
                raise ImportError("Failed to import dill, magic Jython function serialization " +
                                  "depends on dill on the driver machine. You may wish to pass " +
                                  "your function in as a string instead.")
            try:
                src = dill.source.getsource(func)
            except:
                print("Failed to get the source code associated with provided function. " +
                      "You may wish to try and assign you lambda to a variable or pass in as a " +
                      "string.")
                raise
            # Extract the globals, classes, etc. needed for this function
            file = StringIO()
            cp = cloudpickle.CloudPickler(file)
            code, f_globals, defaults, closure, dct, base_globals = cp.extract_func_data(func)
            closure_dct = {}
            if func.__closure__:
                if sys.version < "3":
                    closure_dct = dict(zip(func.func_code.co_freevars,
                                           (c.cell_contents for c in func.func_closure)))
                else:
                    closure_dct = dict(zip(func.__code__.co_freevars,
                                           (c.cell_contents for c in func.__closure__)))

            req = dict(base_globals)
            req.update(f_globals)
            req.update(closure_dct)
            # Serialize the "extras" and drop PySpark imports
            ser = CloudPickleSerializer()

            def isClass(v):
                return isinstance(v, (type, types.ClassType))

            def isInternal(v):
                return v.__module__.startswith("pyspark")

            # Sort out PySpark and non PySpark requirements
            req_vars = dict((k, v) for k, v in req.items() if not isClass(v) or not isInternal(v))
            req_imports = dict((k, v) for k, v in req.items() if isClass(v) and isInternal(v))
            if req_vars:
                serialized_vars = b64encode(ser.dumps(req_vars)).decode("utf-8")
            if req_imports:
                formatted_imports = list((v.__module__, v.__name__, k)
                                         for k, v in req_imports.items())
                serialized_imports = b64encode(ser.dumps(formatted_imports)).decode("utf-8")

        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        jdt = spark._jsparkSession.parseDataType(self.returnType.json())
        if name is None:
            f = self.func
            name = f.__name__ if hasattr(f, '__name__') else f.__class__.__name__
        # Create a Java representation
        wrapped_jython_func = _wrap_jython_func(sc, src, serialized_vars, serialized_imports,
                                                self.setupCode)
        judf = sc._jvm.org.apache.spark.sql.jython.UserDefinedJythonFunction(
            name, wrapped_jython_func, jdt)
        return judf

    def __del__(self):
        try:
            self._judf.func().lazyFunc().unpersist(False)
        except:
            # Exception happens if shutting down, doesn't matter.
            pass

    def __call__(self, *cols):
        sc = SparkContext._active_spark_context
        jc = self._judf.apply(_to_seq(sc, cols, _to_java_column))
        return Column(jc)
