package net.wrap_trap.calcite_sample

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelProtoDataType}
import org.apache.calcite.schema.impl.AbstractTable

/**
  * Created by masayuki on 2017/03/06.
  */
class PojoTable(protoRowType: RelProtoDataType) extends AbstractTable {

  var fieldTypes: Option[List[FieldType]] = None

  def getFieldTypes(): Option[List[FieldType]] = fieldTypes

  def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    if (this.protoRowType != null) {
      return this.protoRowType.apply(typeFactory)
    }
    val (retFieldTypes: List[FieldType], relDataType: RelDataType) =
      PojoEnumerator.deduceRowType(typeFactory.asInstanceOf[JavaTypeFactory])
    this.fieldTypes = Option(retFieldTypes)
    relDataType
  }
}
