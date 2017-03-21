package net.wrap_trap.calcite_sample

/**
  * Created by masayuki on 2017/03/21.
  */
case object Dept {
  val FIELD_TYPES: Array[(String, String, FieldType)] = Array(
    ("DEPTNO", "deptNo", INT),
    ("NAME", "name", STRING),
    ("JOINDATE", "joinDate", DATE))
}

case class Dept(deptNo: Int, name: String, deptId: Int, joinDate: java.sql.Date) {}