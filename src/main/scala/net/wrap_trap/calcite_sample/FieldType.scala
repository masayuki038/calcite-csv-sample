package net.wrap_trap.calcite_sample

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.linq4j.tree.Primitive

/**
  * Created by masayuki on 2017/02/18.
  */
sealed abstract class FieldType()
case object FieldType {
  val map = List(
    ("string", STRING), ("boolean", BOOLEAN), ("byte", BYTE), ("char", CHAR), ("short", SHORT), ("int", INT),
    ("long", LONG), ("float", FLOAT), ("double", DOUBLE), ("date", DATE), ("time", TIME), ("timestamp", TIMESTAMP))
    .toMap[String, FieldType]

  def toType(csvFieldType: FieldType, typeFactory: JavaTypeFactory): RelDataType = {
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

  def of(typeString: String): Option[FieldType] = {
    map.get(typeString)
  }
}
case object STRING extends FieldType
case object BOOLEAN extends FieldType
case object BYTE extends FieldType
case object CHAR extends FieldType
case object SHORT extends FieldType
case object INT extends FieldType
case object LONG extends FieldType
case object FLOAT extends FieldType
case object DOUBLE extends FieldType
case object DATE extends FieldType
case object TIME extends FieldType
case object TIMESTAMP extends FieldType
