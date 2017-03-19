package net.wrap_trap.calcite_sample

import org.apache.calcite.schema.Table
import org.apache.calcite.schema.impl.AbstractSchema

/**
  * Created by masayuki on 2017/03/19.
  */
class PojoSchema extends AbstractSchema {

  override def getTableMap(): java.util.Map[String, Table] = {
    val map = new java.util.HashMap[String, Table]
    map.put("EMP", new PojoScannableTable(null))
    map
  }
}
