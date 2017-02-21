package net.wrap_trap.calcite_sample

import java.io.{File, FilenameFilter}

import com.google.common.collect.ImmutableMap
import org.apache.calcite.schema.Table
import org.apache.calcite.schema.impl.AbstractSchema
import org.apache.calcite.util.ImmutableNullableList

/**
  * Created by masayuki on 2017/02/21.
  */
object CsvSchema {

}

class CsvSchema(val directoryFile: File, flavor: Flavor) extends AbstractSchema {

  private def trim(s: String, suffix: String): String = {
    val trimmed = trimOrNull(s, suffix)
    trimmed match {
      case null => s
      case _ => trimmed
    }
  }

  private def trimOrNull(s: String, suffix: String): String = {
    s.endsWith(suffix) match {
      case true => s.substring(0, s.length() - suffix.length)
      case false => null
    }
  }

  override def getTableMap(): java.util.Map[String, Table] = {
    var files: Array[File] = directoryFile.listFiles((dir: File, name: String) => {
      val nameSansGz = trim(name, ".gz")
      nameSansGz.endsWith(".csv") || nameSansGz.endsWith(".json")
    })

    if(files == null) {
      System.out.println("directory " + directoryFile + " not found")
      files = new Array[File](0)
    }

    val map = new java.util.HashMap[String, Table]
    files.foreach(file => {
      var tableName = trim(file.getName, ".gz")
      tableName = trim(tableName, ".csv")

      val table = createTable(file)
      map.put(tableName, table)
    })
    map
  }

  private def createTable(file: File): Table = {
    flavor match {
      case Translatable => new CsvTranslatableTable(file, null)
      case Scannable => new CsvScannableTable(file, null)
      case Filterable => new CsvFilterableTable(file, null)
    }
  }
}
