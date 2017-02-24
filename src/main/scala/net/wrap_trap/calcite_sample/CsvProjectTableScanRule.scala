package net.wrap_trap.calcite_sample

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rex.{RexInputRef, RexNode}
import collection.JavaConversions._

/**
  * Created by masayuki on 2017/02/24.
  */
object CsvProjectTableScanRule {
  val INSTANCE = new CsvProjectTableScanRule()
}

class CsvProjectTableScanRule
  extends RelOptRule(
    RelOptRule.operand(
      classOf[LogicalProject],
      RelOptRule.operand(classOf[CsvTableScan], RelOptRule.none())
    ), "CsvProjectTableScanRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val project = call.rel[LogicalProject](0)
    val scan = call.rel[CsvTableScan](1)
    val fields = getProjectFields(project.getProjects)
    if (fields == null) {
      return
    }

    call.transformTo(
      new CsvTableScan(
        scan.getCluster,
        scan.getTable,
        scan.csvTable,
        fields
      )
    )
  }

  def getProjectFields(exps: java.util.List[RexNode]): Array[Int] = {
    val fields = new Array[Int](exps.size)
    exps.zipWithIndex.foreach{ case (exp: RexNode, i: Int) => {
      if (exp.isInstanceOf[RexInputRef]) {
        fields(i) = exp.asInstanceOf[RexInputRef].getIndex
      } else {
        return null
      }
    }}
    return fields
  }
}
