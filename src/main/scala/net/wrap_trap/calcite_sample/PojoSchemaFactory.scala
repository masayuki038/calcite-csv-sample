package net.wrap_trap.calcite_sample

import org.apache.calcite.schema.{Schema, SchemaFactory, SchemaPlus}

/**
  * Created by masayuki on 2017/03/19.
  */
class PojoSchemaFactory extends SchemaFactory {
  override def create(parentSchema: SchemaPlus, name: String, operand: java.util.Map[String, Object]): Schema = {
    new PojoSchema
  }
}
