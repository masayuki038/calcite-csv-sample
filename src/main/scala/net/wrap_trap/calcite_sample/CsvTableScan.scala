package net.wrap_trap.calcite_sample

import org.apache.calcite.adapter.enumerable.EnumerableRel.{Prefer, Result}
import org.apache.calcite.adapter.enumerable.{EnumerableConvention, EnumerableRelImplementor, PhysTypeImpl}
import org.apache.calcite.linq4j.tree.{Blocks, Expression, Expressions, Primitive}
import org.apache.calcite.plan.{RelOptCluster, RelOptPlanner, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rel.core.TableScan

/**
  * Created by masayuki on 2017/02/24.
  */
class CsvTableScan(val cluster: RelOptCluster,
                   val myTable: RelOptTable,
                   val csvTable: CsvTranslatableTable,
                   val fields: Array[Int])
  extends TableScan(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), myTable) with RelOptTable {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new CsvTableScan(getCluster(), myTable, csvTable, fields)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("fields", Primitive.asList(fields))
  }

  override def deriveRowType(): RelDataType = {
    val fieldList = myTable.getRowType.getFieldList
    val builder = getCluster.getTypeFactory.builder
    fields.foreach(field => builder.add(fieldList.get(field)))
    builder.build
  }

  override def register(planner: RelOptPlanner): Unit = {
    planner.addRule(CsvProjectTableScanRule.INSTANCE)
  }

  def implement(implementor: EnumerableRelImplementor, pref: Prefer): Result ={
    val physType = PhysTypeImpl.of(implementor.getTypeFactory, getRowType, pref.preferArray)
    implementor.result(physType, Blocks.toBlock(
      Expressions.call(
        table.getExpression(classOf[CsvTranslatableTable]),
        "project",
        implementor.getRootExpression,
        Expressions.constant(fields))
    ))
  }
}
