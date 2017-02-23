package net.wrap_trap.calcite_sample

import java.io.File

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.{AbstractEnumerable, Enumerable, Enumerator}
import org.apache.calcite.rel.`type`.RelProtoDataType
import org.apache.calcite.rex.RexNode

/**
  * Created by masayuki on 2017/02/21.
  */
class CsvScannableTable(val tFile: File, val tProtoRowType: Option[RelProtoDataType])
  extends CsvTable(tFile, tProtoRowType) {

  override def toString(): String = {
    "CsvScannableTable"
  }

  def scan(root: DataContext, filterCandidate: List[RexNode]): Enumerable[Array[Any]] = {
    val fieldTypes = getFieldTypes.get
    val fields = CsvEnumerator.identityList(fieldTypes.size)
    val cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root)
    new AbstractEnumerable[Array[Any]] {
      override def enumerator(): Enumerator[Array[Any]] = {
        new CsvEnumerator(file, cancelFlag, null, new ArrayRowConverter(fieldTypes.toArray, fields))
      }
    }
  }
}