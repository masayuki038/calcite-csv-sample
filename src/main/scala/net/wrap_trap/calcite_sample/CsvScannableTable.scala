package net.wrap_trap.calcite_sample

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.{AbstractEnumerable, Enumerable, Enumerator}
import org.apache.calcite.rel.`type`.RelProtoDataType
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.ScannableTable

/**
  * Created by masayuki on 2017/02/21.
  */
class CsvScannableTable(val tFile: File, val tProtoRowType: RelProtoDataType)
  extends CsvTable(tFile, tProtoRowType) with ScannableTable {

  override def toString(): String = {
    "CsvScannableTable"
  }

  override def scan(root: DataContext): Enumerable[Array[Object]] = {
    val fieldTypes = getFieldTypes.get
    val fields = EnumeratorUtils.identityList(fieldTypes.size)
    val cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root).asInstanceOf[AtomicBoolean]
    new AbstractEnumerable[Array[Object]] {
      override def enumerator(): Enumerator[Array[Object]] = {
        new CsvEnumerator(file, cancelFlag, null, new ArrayRowConverter(fieldTypes.toArray, fields))
      }
    }
  }
}