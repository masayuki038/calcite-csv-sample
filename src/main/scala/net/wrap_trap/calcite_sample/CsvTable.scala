package net.wrap_trap.calcite_sample

import java.io.File

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelProtoDataType}
import org.apache.calcite.schema.impl.AbstractTable

/**
  * Created by masayuki on 2017/02/17.
  */
class CsvTable(val file: File, protoRowType: Option[RelProtoDataType]) extends AbstractTable {

  var fieldTypes: Option[List[CsvFieldType]] = None

  def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    if (this.protoRowType.isDefined) {
      return this.protoRowType.get.apply(typeFactory)
    }
    if (fieldTypes.isEmpty) {
      this.fieldTypes = Option(List.empty[CsvFieldType])
      return CsvEnumerator.deduceRowType(typeFactory.asInstanceOf[JavaTypeFactory], file, fieldTypes)
    } else {
      return CsvEnumerator.deduceRowType(typeFactory.asInstanceOf[JavaTypeFactory], file, None)
    }
  }
}

sealed abstract class Flavor
case object Scannable extends Flavor
case object Filterable extends Flavor
case object Translatable extends Flavor

