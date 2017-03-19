package net.wrap_trap.calcite_sample

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.{AbstractEnumerable, Enumerable, Enumerator}
import org.apache.calcite.rel.`type`.RelProtoDataType
import org.apache.calcite.schema.ScannableTable

/**
  * Created by masayuki on 2017/03/06.
  */
class PojoScannableTable(val tProtoRowType: RelProtoDataType)
  extends PojoTable(tProtoRowType) with ScannableTable {

  val empMap = createEmpMap

  def createEmpMap(): scala.collection.mutable.Map[String, Object] = {
    val map  = scala.collection.mutable.Map.empty[String, Object]
    map.put("1", Emp(1, "emp1", 1, "M", "city1", 1, 30, false, false, new java.sql.Date(System.currentTimeMillis)))
    map.put("2", Emp(1, "emp2", 2, "M", "city1", 2, 40, false, false, new java.sql.Date(System.currentTimeMillis)))
    map.put("3", Emp(1, "emp3", 3, "F", "city1", 3, 50, false, false, new java.sql.Date(System.currentTimeMillis)))
    map.put("4", Emp(1, "emp4", 2, "F", "city1", 4, 60, false, false, new java.sql.Date(System.currentTimeMillis)))
    map.put("5", Emp(1, "emp5", 1, "M", "city1", 5, 70, false, false, new java.sql.Date(System.currentTimeMillis)))
    map
  }

  override def toString(): String = {
    "PojoScannableTable"
  }

  override def scan(root: DataContext): Enumerable[Array[Object]] = {
    val fieldTypes = getFieldTypes.get
    val fields = EnumeratorUtils.identityList(fieldTypes.size)
    new AbstractEnumerable[Array[Object]] {
      override def enumerator(): Enumerator[Array[Object]] = {
        new PojoEnumerator(empMap, new PojoRowConverter(fieldTypes.toArray, fields))
      }
    }
  }
}
