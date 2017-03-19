package net.wrap_trap.calcite_sample

import java.util.TimeZone

import com.fasterxml.jackson.databind.util.BeanUtil
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.linq4j.Enumerator
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.util.Pair
import org.apache.commons.beanutils.BeanUtils
import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by masayuki on 2017/03/06.
  */
object PojoEnumerator {
  val gmt = TimeZone.getTimeZone("GMT")
  val TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt)
  val TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt)
  val TIME_FORMAT_TIMESTAMP = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt)

  def deduceRowType(typeFactory: JavaTypeFactory): RelDataType = {
    var names = List.empty[String]
    var relDataTypes = List.empty[RelDataType]

    Emp.FIELD_TYPES.foreach{case (name: String, _, fieldType: FieldType) => {
      val relDataType = FieldType.toType(fieldType, typeFactory)
      names = name :: names
      relDataTypes = relDataType :: relDataTypes
    }}

    typeFactory.createStructType(Pair.zip(names.reverse.toArray, relDataTypes.reverse.toArray))
  }
}

class PojoEnumerator(val map: scala.collection.mutable.Map[String, Object],
                     val fields: Array[Int]) extends Enumerator[Array[Object]] {
  val iterator = map.iterator
  var currentPos: Option[Array[Object]] = None

  override def current(): Array[Object] = {
    currentPos.get
  }

  override def moveNext(): Boolean = {
    if(!iterator.hasNext)  {
      this.currentPos = None
      return false
    }
    val (_, pojo) = iterator.next
    this.currentPos = Option(convertRow(pojo))
    return true
  }

  override def reset(): Unit = {
    throw new UnsupportedOperationException
  }

  override def close(): Unit = {}

  def convertRow(pojo: Object): Array[Object] = {
    val objects = new Array[Object](fields.length)
    var i = 0
    fields.foreach(field => {
      val property = classOf[Emp].getDeclaredField(Emp.FIELD_TYPES(field)._2)
      property.setAccessible(true)
      objects(i) = property.get(pojo)
      i += 1
    })
    objects
  }
}
