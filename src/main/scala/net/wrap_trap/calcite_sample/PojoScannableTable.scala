package net.wrap_trap.calcite_sample

import java.io.File

import org.apache.calcite.rel.`type`.RelProtoDataType
import org.apache.calcite.schema.ScannableTable

/**
  * Created by masayuki on 2017/03/06.
  */
class PojoScannableTable(val tProtoRowType: RelProtoDataType)
  extends PojoTable(tProtoRowType) with ScannableTable {

}
