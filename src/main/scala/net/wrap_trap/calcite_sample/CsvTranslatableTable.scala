package net.wrap_trap.calcite_sample

import java.io.File

import org.apache.calcite.rel.`type`.RelProtoDataType

/**
  * Created by masayuki on 2017/02/21.
  */
class CsvTranslatableTable(val tFile: File, val tProtoRowType: Option[RelProtoDataType])
  extends CsvTable(tFile, tProtoRowType) {
}
