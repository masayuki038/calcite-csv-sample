package net.wrap_trap.calcite_sample

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.linq4j.tree.Primitive

/**
  * Created by masayuki on 2017/02/18.
  */
sealed abstract class CsvFieldType()
case object CsvFieldType {
  val map = List(
    ("string", STRING), ("boolean", BOOLEAN), ("byte", BYTE), ("char", CHAR), ("short", SHORT), ("int", INT),
    ("long", LONG), ("float", FLOAT), ("double", DOUBLE), ("date", DATE), ("time", TIME), ("timestamp", TIMESTAMP))
    .toMap[String, CsvFieldType]

  def toType(csvFieldType: CsvFieldType, typeFactory: JavaTypeFactory): RelDataType = {
    val clazz = csvFieldType match {
      case STRING => classOf[String]
      case BOOLEAN => Primitive.BOOLEAN.boxClass
      case BYTE => Primitive.BYTE.boxClass
      case CHAR => Primitive.CHAR.boxClass
      case SHORT => Primitive.SHORT.boxClass
      case INT => Primitive.INT.boxClass
      case LONG => Primitive.LONG.boxClass
      case FLOAT => Primitive.FLOAT.boxClass
      case DOUBLE => Primitive.DOUBLE.boxClass
      case DATE => classOf[java.sql.Date]
      case TIME => classOf[java.sql.Time]
      case TIMESTAMP => classOf[java.sql.Timestamp]
    }
    typeFactory.createJavaType(clazz)
  }

  def of(typeString: String): Option[CsvFieldType] = {
    map.get(typeString)
  }
}
case object STRING extends CsvFieldType
case object BOOLEAN extends CsvFieldType
case object BYTE extends CsvFieldType
case object CHAR extends CsvFieldType
case object SHORT extends CsvFieldType
case object INT extends CsvFieldType
case object LONG extends CsvFieldType
case object FLOAT extends CsvFieldType
case object DOUBLE extends CsvFieldType
case object DATE extends CsvFieldType
case object TIME extends CsvFieldType
case object TIMESTAMP extends CsvFieldType
