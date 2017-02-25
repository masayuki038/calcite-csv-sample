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
object CsvEnumerator {
  val gmt = TimeZone.getTimeZone("GMT")
  val TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt)
  val TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt)
  val TIME_FORMAT_TIMESTAMP = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt)

  def deduceRowType(typeFactory: JavaTypeFactory, file: File, fieldTypes: Option[List[CsvFieldType]]): RelDataType = {
    var types = List.empty[RelDataType]
    var names = List.empty[String]
    var retFieldTypes = fieldTypes match {
      case Some(t) => t
      case None => List.empty[CsvFieldType]
    }

    val reader = openCsv(file)
    val strings = reader.readNext()
    strings.foreach(string => {
      val colon = string.indexOf(":")
      val (name, fieldType) = (colon >= 0) match {
        case true => {
          val name = string.substring(0, colon)
          val typeString = string.substring(colon + 1)
          val fieldType = CsvFieldType.of(typeString)
          (name, fieldType)
        }
        case _ => {
          (string, None)
        }
      }
      val typ = fieldType match {
        case Some(f) => {
          retFieldTypes = f :: retFieldTypes
          CsvFieldType.toType(f, typeFactory)
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
    typeFactory.createStructType(Pair.zip(names.toArray, types.toArray))
  }

  private def openCsv(file: File): CSVReader = {
    val reader = file.getName.endsWith(".gz") match {
      case true => new InputStreamReader(new GZIPInputStream(new FileInputStream(file)))
      case _ => new FileReader(file)
    }
    new CSVReader(reader)
  }

  def converter(fieldTypes: Array[CsvFieldType], fields: Array[Int]): RowConverter[Array[Any]] = {
    new ArrayRowConverter(fieldTypes, fields)
  }

  def identityList(n: Int): Array[Int] = {
    (0 to n).toArray
  }
}

class CsvEnumerator(val file: File,
                       val cancelFlag: AtomicBoolean,
                       val filterValues: Array[String],
                       val rowConverter: RowConverter[Array[Any]]) extends Enumerator[Array[Any]] {
  var csvReader: CSVReader = CsvEnumerator.openCsv(file)
  var currentPos: Option[Array[Any]] = None
  this.csvReader.readNext()

  def this(file: File, cancelFlag: AtomicBoolean, fieldTypes: List[CsvFieldType], fields: Array[Int]) = {
    this(file, cancelFlag, Array.empty[String], CsvEnumerator.converter(fieldTypes.toArray, fields))
  }

  override def current(): Array[Any] = {
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
      if (this.filterValues.length > 0) {
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

abstract class RowConverter[+E] {
  def convertRow(rows: Array[String]): E

  def convert(fieldType: Option[CsvFieldType], string: String): Option[Any] = {
    fieldType match {
      case None => Option(string)
      case Some(BOOLEAN) => {
        if (string.length == 0) {
          None
        } else {
          Option(java.lang.Boolean.parseBoolean(string))
        }
      }
      case Some(BYTE) => {
        if (string.length == 0) {
          None
        } else {
          Option(java.lang.Byte.parseByte(string))
        }
      }
      case Some(CHAR) => {
        if (string.length == 0) {
          None
        } else {
          Option(string)
        }
      }
      case Some(SHORT) => {
        if (string.length == 0) {
          None
        } else {
          Option(java.lang.Short.parseShort(string))
        }
      }
      case Some(INT) => {
        if (string.length == 0) {
          None
        } else {
          Option(java.lang.Integer.parseInt(string))
        }
      }
      case Some(LONG) => {
        if (string.length == 0) {
          None
        } else {
          Option(java.lang.Long.parseLong(string))
        }
      }
      case Some(FLOAT) => {
        if (string.length == 0) {
          None
        } else {
          Option(java.lang.Float.parseFloat(string))
        }
      }
      case Some(DOUBLE) => {
        if (string.length == 0) {
          None
        } else {
          Option(java.lang.Double.parseDouble(string))
        }
      }
      case Some(DATE) => {
        if (string.length == 0) {
          None
        } else {
          try {
            val date = CsvEnumerator.TIME_FORMAT_DATE.parse(string)
            Option(new java.sql.Date(date.getTime))
          } catch {
            case _: Throwable => None
          }
        }
      }
      case Some(TIME) => {
        if (string.length == 0) {
          None
        } else {
          try {
            val time = CsvEnumerator.TIME_FORMAT_TIME.parse(string)
            Option(new java.sql.Time(time.getTime))
          } catch {
            case _: Throwable => None
          }
        }
      }
      case Some(TIMESTAMP) => {
        if (string.length == 0) {
          None
        } else {
          try {
            val timestamp = CsvEnumerator.TIME_FORMAT_TIMESTAMP.parse(string)
            Option(new Timestamp(timestamp.getTime))
          } catch {
            case _: Throwable => None
          }
        }
      }
      case Some(STRING) => Option(string)
    }
  }
}

class ArrayRowConverter(val fieldTypes: Array[CsvFieldType], val fields: Array[Int]) extends RowConverter[Array[Any]] {
  override def convertRow(strings: Array[String]): Array[Any] = {
    val objects = new Array[Any](fields.length + 1)
    var i = 0
    objects(i) = System.currentTimeMillis
    i += 1
    fields.foreach(field => {
      objects(i) = convert(Option(fieldTypes(field)), strings(field))
      i += 1
    })
    objects
  }
}