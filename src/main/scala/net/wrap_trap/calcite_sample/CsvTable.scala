package net.wrap_trap.calcite_sample

import java.io.File

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelProtoDataType}
import org.apache.calcite.schema.impl.AbstractTable

/**
  * Created by masayuki on 2017/02/17.
  */
class CsvTable(val file: File, protoRowType: RelProtoDataType) extends AbstractTable {

  var fieldTypes: Option[List[FieldType]] = None

  def getFieldTypes(): Option[List[FieldType]] = fieldTypes

  def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    if (this.protoRowType != null) {
      return this.protoRowType.apply(typeFactory)
    }
    val (retFieldTypes: List[FieldType], relDataType: RelDataType) = if (fieldTypes.isEmpty) {
      this.fieldTypes = Option(List.empty[FieldType])
      CsvEnumerator.deduceRowType(typeFactory.asInstanceOf[JavaTypeFactory], file, fieldTypes)
    } else {
      CsvEnumerator.deduceRowType(typeFactory.asInstanceOf[JavaTypeFactory], file, None)
    }
    this.fieldTypes = Option(retFieldTypes)
    relDataType
  }
}

sealed abstract class Flavor
case object Scannable extends Flavor
case object Filterable extends Flavor
case object Translatable extends Flavor

