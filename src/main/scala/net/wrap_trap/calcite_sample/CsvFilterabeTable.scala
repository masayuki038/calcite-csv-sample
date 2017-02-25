package net.wrap_trap.calcite_sample

import java.io.File

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.{AbstractEnumerable, Enumerable, Enumerator}
import org.apache.calcite.rel.`type`.RelProtoDataType
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.sql.SqlKind

/**
  * Created by masayuki on 2017/02/21.
  */
class CsvFilterableTable(val tFile: File, val tProtoRowType: Option[RelProtoDataType])
  extends CsvTable(tFile, tProtoRowType) {

  override def toString(): String = {
    "CsvFilterableTable"
  }

  def scan(root: DataContext, filterCandidate: List[RexNode]): Enumerable[Array[Any]] = {
    val fields = CsvEnumerator.identityList(fieldTypes.size)
    val filterValues = new Array[String](fieldTypes.size)

    filterCandidate.foreach(
      filter => isFilterApplicable(filter).foreach(f => filterValues(f._1) = f._2.getValue2.toString))

    val cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root)
    new AbstractEnumerable[Array[Any]] {
      override def enumerator(): Enumerator[Array[Any]] = {
        val fieldTypeList = getFieldTypes.get
        new CsvEnumerator(
          file, cancelFlag, filterValues, new ArrayRowConverter(fieldTypeList.toArray, fields)
        )
      }
    }
  }

  def isFilterApplicable(filter: RexNode): Option[(Int, RexLiteral)] = {
    if (filter.isA(SqlKind.EQUALS)) {
      val call = filter.asInstanceOf[RexCall]
      var left = call.getOperands.get(0)
      if (left.isA(SqlKind.CAST)) {
        left = left.asInstanceOf[RexCall].operands.get(0)
      }
      val right = call.getOperands.get(1)
      if (left.isInstanceOf[RexInputRef] && right.isInstanceOf[RexLiteral]) {
        return Option(left.asInstanceOf[RexInputRef].getIndex, right.asInstanceOf[RexLiteral])
      }
    }
    None
  }
}
