package net.wrap_trap.calcite_sample

import java.io.File
import java.lang.reflect.Type

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.linq4j._
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelProtoDataType
import org.apache.calcite.schema.{SchemaPlus, Schemas}

/**
  * Created by masayuki on 2017/02/21.
  */
class CsvTranslatableTable(val tFile: File, val tProtoRowType: RelProtoDataType)
  extends CsvTable(tFile, tProtoRowType) {

  override def toString(): String = {
    "CsvTranslatableTable"
  }

  def project(root: DataContext, fields: Array[Int]): Enumerable[Array[Object]] = {
    val fieldTypes = getFieldTypes.get

    val cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root)
    new AbstractEnumerable[Array[Object]] {
      override def enumerator(): Enumerator[Array[Object]] = {
        new CsvEnumerator(file, cancelFlag, fieldTypes, fields)
      }
    }
  }

  def getExpression[T](schema: SchemaPlus, tableName: String, clazz: Class[T]): Expression = {
    Schemas.tableExpression(schema, getElementType, tableName, clazz)
  }

  def getElementType(): Type = classOf[Array[Any]]

  def asQueryable[T](queryProvider: QueryProvider, schema: SchemaPlus, tableName: String): Queryable[T] = {
    throw new UnsupportedOperationException()
  }

  def toRel(context: RelOptTable.ToRelContext, relOptTable: RelOptTable): RelNode = {
    val fieldCount = relOptTable.getRowType.getFieldCount
    val fields = CsvEnumerator.identityList(fieldCount)
    new CsvTableScan(context.getCluster, relOptTable, this, fields)
  }
}
