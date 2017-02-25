package net.wrap_trap.calcite_sample

import java.io.File

import org.apache.calcite.model.ModelHandler
import org.apache.calcite.schema.{Schema, SchemaFactory, SchemaPlus}

/**
  * Created by masayuki on 2017/02/21.
  */
object CsvSchemaFactory {
  val ROWTIME_COLUMN_NAME = "ROWTIME"

  lazy val _instance = {
    new CsvSchemaFactory()
  }

  def getInstance(): Unit = {
    _instance
  }
}
class CsvSchemaFactory extends SchemaFactory {
  override def create(parentSchema: SchemaPlus, name: String, operand: java.util.Map[String, Object]): Schema = {
    val directory = operand.get("directory").asInstanceOf[String]
    val base = operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName).asInstanceOf[File]
    var directoryFile = new File(directory)
    if (base != null && !directoryFile.isAbsolute) {
      directoryFile = new File(base, directory)
    }
    val flavorName = operand.get("flavor").asInstanceOf[String]
    val flavor = flavorName match {
      case null => Scannable
      case "scannable" => Scannable
      case "filterable" => Filterable
      case "translatable" => Translatable
    }
    new CsvSchema(directoryFile, flavor)
  }
}
