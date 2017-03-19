package net.wrap_trap.calcite_sample

import org.apache.calcite.DataContext
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.linq4j.{AbstractEnumerable, Enumerable, Enumerator}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelProtoDataType}
import org.apache.calcite.schema.ScannableTable
import org.apache.calcite.schema.impl.AbstractTable

/**
  * Created by masayuki on 2017/03/06.
  */
object PojoScannableTable {
  def createEmpMap(): scala.collection.mutable.Map[String, Object] = {
    val map  = scala.collection.mutable.Map.empty[String, Object]
    map.put("1", Emp(1, "emp1", 1, "M", "city1", 1, 30, false, false, new java.sql.Date(System.currentTimeMillis)))
    map.put("2", Emp(1, "emp2", 2, "M", "city1", 2, 40, false, false, new java.sql.Date(System.currentTimeMillis)))
    map.put("3", Emp(1, "emp3", 3, "F", "city1", 3, 50, false, false, new java.sql.Date(System.currentTimeMillis)))
    map.put("4", Emp(1, "emp4", 2, "F", "city1", 4, 60, false, false, new java.sql.Date(System.currentTimeMillis)))
    map.put("5", Emp(1, "emp5", 1, "M", "city1", 5, 70, false, false, new java.sql.Date(System.currentTimeMillis)))
    map
  }
}


class PojoScannableTable(val tProtoRowType: RelProtoDataType)
  extends AbstractTable with ScannableTable {

  val empMap = PojoScannableTable.createEmpMap

  override def toString(): String = {
    "PojoScannableTable"
  }

  def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    if (this.tProtoRowType != null) {
      return this.tProtoRowType.apply(typeFactory)
    }
    PojoEnumerator.deduceRowType(typeFactory.asInstanceOf[JavaTypeFactory])
  }

  override def scan(root: DataContext): Enumerable[Array[Object]] = {
    val fields = EnumeratorUtils.identityList(Emp.FIELD_TYPES.size)
    new AbstractEnumerable[Array[Object]] {
      override def enumerator(): Enumerator[Array[Object]] = {
        new PojoEnumerator(empMap, fields)
      }
    }
  }
}
