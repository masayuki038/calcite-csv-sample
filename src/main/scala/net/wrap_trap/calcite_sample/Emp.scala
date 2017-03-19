package net.wrap_trap.calcite_sample

import org.apache.calcite.rel.`type`.RelDataType

/**
  * Created by masayuki on 2017/03/06.
  */

case object Emp {
  val FIELD_TYPES: Array[(String, String, FieldType)] = Array(
    ("EMPNO", "empNo", INT),
    ("NAME", "name", STRING),
    ("DEPTNO", "deptNo", INT),
    ("GENDER", "gender", STRING),
    ("CITY", "city", STRING),
    ("EMPID", "empId", INT),
    ("AGE", "age", INT),
    ("SLACKER", "slacker", BOOLEAN),
    ("MANAGER", "manager", BOOLEAN),
    ("JOINDATE", "joinDate", DATE))
}


case class Emp(empNo: Int, name: String, deptNo: Int, gender: String, city: String, emptId: Int, age: Int,
               slacker: Boolean, manager: Boolean, joinDate: java.sql.Date) {}
