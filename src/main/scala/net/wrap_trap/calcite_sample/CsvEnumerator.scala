package net.wrap_trap.calcite_sample

import java.io._
import java.sql.Timestamp
import java.util.TimeZone
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.GZIPInputStream

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.linq4j.Enumerator
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.commons.lang3.time.FastDateFormat
import au.com.bytecode.opencsv.CSVReader
import org.apache.calcite.util.Pair
/**
  * Created by masayuki on 2017/02/18.
  */
object EnumeratorUtils {
  def converter(fieldTypes: Array[FieldType], fields: Array[Int]): RowConverter[Array[String], Array[Object]] = {
    new ArrayRowConverter(fieldTypes, fields)
  }

  def identityList(n: Int): Array[Int] = {
    (0 to n-1).toArray
  }
}

object CsvEnumerator {
  val gmt = TimeZone.getTimeZone("GMT")
  val TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt)
  val TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt)
  val TIME_FORMAT_TIMESTAMP = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt)

  def deduceRowType(typeFactory: JavaTypeFactory, file: File, fieldTypes: Option[List[FieldType]]): (List[FieldType], RelDataType) = {
    var types = List.empty[RelDataType]
    var names = List.empty[String]
    var retFieldTypes = fieldTypes match {
      case Some(t) => t
      case None => List.empty[FieldType]
    }

    val reader = openCsv(file)
    val strings = reader.readNext()
    strings.foreach(string => {
      val colon = string.indexOf(":")
      val (name, fieldType) = (colon >= 0) match {
        case true => {
          val name = string.substring(0, colon)
          val typeString = string.substring(colon + 1)
          val fieldType = FieldType.of(typeString)
          (name, fieldType)
        }
        case _ => {
          (string, None)
        }
      }
      val typ = fieldType match {
        case Some(f) => {
          retFieldTypes = f :: retFieldTypes
          FieldType.toType(f, typeFactory)
        }
        case None => typeFactory.createJavaType(classOf[String])
      }
      names = name :: names
      types = typ :: types
    })
    if (names.isEmpty) {
      names = "line" :: names
      types = typeFactory.createJavaType(classOf[String]) :: types
    }
    (retFieldTypes.reverse, typeFactory.createStructType(Pair.zip(names.reverse.toArray, types.reverse.toArray)))
  }

  private def openCsv(file: File): CSVReader = {
    val reader = file.getName.endsWith(".gz") match {
      case true => new InputStreamReader(new GZIPInputStream(new FileInputStream(file)))
      case _ => new FileReader(file)
    }
    new CSVReader(reader)
  }
}

class CsvEnumerator(val file: File,
                       val cancelFlag: AtomicBoolean,
                       val filterValues: Array[String],
                       val rowConverter: RowConverter[Array[String], Array[Object]]) extends Enumerator[Array[Object]] {
  var csvReader: CSVReader = CsvEnumerator.openCsv(file)
  var currentPos: Option[Array[Object]] = None
  this.csvReader.readNext()

  def this(file: File, cancelFlag: AtomicBoolean, fieldTypes: List[FieldType], fields: Array[Int]) = {
    this(file, cancelFlag, Array.empty[String], EnumeratorUtils.converter(fieldTypes.toArray, fields))
  }

  override def current(): Array[Object] = {
    currentPos.get
  }

  override def moveNext(): Boolean = {
    while(true) {
      if (cancelFlag.get) {
        return false
      }
      val strings = csvReader.readNext()
      if(strings == null) {
        currentPos = None
        csvReader.close()
        return false
      }
      if (this.filterValues != null && this.filterValues.length > 0) {
        strings.zipWithIndex.foreach { case (str: String, i: Int) => {
          if (str != this.filterValues(i)) {
            return moveNext()
          }
        }}
      }
      this.currentPos = Option(this.rowConverter.convertRow(strings))
      return true
    }
    throw new IllegalStateException("Unexpected flow")
  }

  override def reset(): Unit = {
    throw new UnsupportedOperationException
  }

  override def close(): Unit = {
    csvReader.close
  }
}

abstract class RowConverter[T,+E] {
  def convertRow(target: T): E
  def convert(fieldType: Option[FieldType], value: Object): java.lang.Object
}

class ArrayRowConverter(val fieldTypes: Array[FieldType], val fields: Array[Int])
  extends RowConverter[Array[String], Array[Object]] {
  override def convertRow(fieldValues: Array[String]): Array[Object] = {
    val objects = new Array[Object](fields.length)
    var i = 0
    fields.foreach(field => {
      objects(i) = convert(Option(fieldTypes(field)), fieldValues(field))
      i += 1
    })
    objects
  }

  override def convert(fieldType: Option[FieldType], value: Object): java.lang.Object = {
    if(!value.isInstanceOf[String]) {
      throw new IllegalArgumentException("Unexpected value type: " + value.getClass.getName)
    }
    val string = value.asInstanceOf[String]
    fieldType match {
      case None => string
      case Some(BOOLEAN) => {
        if (string.length == 0) {
          null
        } else {
          new java.lang.Boolean(java.lang.Boolean.parseBoolean(string))
        }
      }
      case Some(BYTE) => {
        if (string.length == 0) {
          null
        } else {
          new java.lang.Byte(java.lang.Byte.parseByte(string))
        }
      }
      case Some(CHAR) => {
        if (string.length == 0) {
          null
        } else {
          string
        }
      }
      case Some(SHORT) => {
        if (string.length == 0) {
          null
        } else {
          new java.lang.Short(java.lang.Short.parseShort(string))
        }
      }
      case Some(INT) => {
        if (string.length == 0) {
          null
        } else {
          new java.lang.Integer(java.lang.Integer.parseInt(string))
        }
      }
      case Some(LONG) => {
        if (string.length == 0) {
          null
        } else {
          new java.lang.Long(java.lang.Long.parseLong(string))
        }
      }
      case Some(FLOAT) => {
        if (string.length == 0) {
          null
        } else {
          new java.lang.Float(java.lang.Float.parseFloat(string))
        }
      }
      case Some(DOUBLE) => {
        if (string.length == 0) {
          null
        } else {
          new java.lang.Double(java.lang.Double.parseDouble(string))
        }
      }
      case Some(DATE) => {
        if (string.length == 0) {
          null
        } else {
          try {
            val date = CsvEnumerator.TIME_FORMAT_DATE.parse(string)
            new java.sql.Date(date.getTime)
          } catch {
            case _: Throwable => null
          }
        }
      }
      case Some(TIME) => {
        if (string.length == 0) {
          null
        } else {
          try {
            val time = CsvEnumerator.TIME_FORMAT_TIME.parse(string)
            new java.sql.Time(time.getTime)
          } catch {
            case _: Throwable => null
          }
        }
      }
      case Some(TIMESTAMP) => {
        if (string.length == 0) {
          null
        } else {
          try {
            val timestamp = CsvEnumerator.TIME_FORMAT_TIMESTAMP.parse(string)
            new Timestamp(timestamp.getTime)
          } catch {
            case _: Throwable => null
          }
        }
      }
      case Some(STRING) => string
    }
  }
}