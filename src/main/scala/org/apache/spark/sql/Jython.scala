package org.apache.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.jython.UserDefinedJythonFunction

/**
  * Created by mariu_000 on 2016-11-09.
  */
object Jython {


  /**
    * Created by mariu_000 on 2016-11-09.
    */

  implicit class JythonUDFRegistration(udfRegistration: UDFRegistration) extends Logging {

    private def functionRegistry: FunctionRegistry = {
      val field = this.udfRegistration.getClass.getDeclaredField("functionRegistry")
      field.get(this.udfRegistration).asInstanceOf[FunctionRegistry]
    }

    protected[sql] def registerJythonUDF(name: String, udf: UserDefinedJythonFunction): Unit = {
      log.debug(
        s"""
           | Registering new JythonUDF:
           | name: $name
           | dataType: ${udf.dataType}
     """.stripMargin)

      functionRegistry.registerFunction(name, udf.builder)
    }
  }
}
