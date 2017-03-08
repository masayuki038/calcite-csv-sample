package net.wrap_trap.calcite_sample

import java.util.TimeZone

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.util.Pair
import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by masayuki on 2017/03/06.
  */
object PojoEnumerator {
  val gmt = TimeZone.getTimeZone("GMT")
  val TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt)
  val TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt)
  val TIME_FORMAT_TIMESTAMP = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt)

  def deduceRowType(typeFactory: JavaTypeFactory, emp: Emp): (List[FieldType], RelDataType) = {
    var retFieldTypes = List.empty[FieldType]
    var names = List.empty[String]
    var relDataTypes = List.empty[RelDataType]

    Emp.FIELD_TYPES.foreach{case (name: String, fieldType: FieldType) => {
      val relDataType = FieldType.toType(fieldType, typeFactory)
      names = name :: names
      relDataTypes = relDataType :: relDataTypes
      retFieldTypes = fieldType :: retFieldTypes
    }}

    (retFieldTypes.reverse, typeFactory.createStructType(Pair.zip(names.reverse.toArray, relDataTypes.reverse.toArray)))
  }
}
