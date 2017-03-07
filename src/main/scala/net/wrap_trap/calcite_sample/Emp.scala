package net.wrap_trap.calcite_sample

import org.apache.calcite.rel.`type`.RelDataType

/**
  * Created by masayuki on 2017/03/06.
  */

case object Emp {
  val FIELD_TYPES: Array[(String, FieldType)] = Array(
    ("EMPNO", INT),
    ("NAME", STRING),
    ("DEPTNO", INT),
    ("GENDER", STRING),
    ("CITY", STRING),
    ("EMPID", INT),
    ("AGE", INT),
    ("SLACKER", BOOLEAN),
    ("MANAGER", BOOLEAN),
    ("JOINDATE", DATE))
}


case class Emp(empNo: Int, name: String, deptNo: Int, gender: String, city: String, emptId: Int, age: Int,
               slacker: Boolean, manager: Boolean, joinDate: java.sql.Date) {}
