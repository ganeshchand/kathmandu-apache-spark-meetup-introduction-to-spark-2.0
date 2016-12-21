package com.gc.meetup.kasm.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * Created by ganeshchand on 12/20/16.
  */

case class Employee(id: Long, dept_id: Long, name: String, age: Int, email: String,sex: String)

object EmployeeDataSet {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Dataset Example").master("local[*]").getOrCreate()

    // data source path
    val csvFilePath = "src/main/resources/employee.csv"
    val jsonFilePath = "src/main/resources/employee.json"
    val parquetFilePath = "src/main/resources/employee.parquet"

    // define a schema - approach 1
    val id         = StructField("id",       DataTypes.LongType)
    val dept_id    = StructField("dept_id",    DataTypes.LongType)
    val name       = StructField("name",   DataTypes.StringType)
    val age        = StructField("age", DataTypes.IntegerType)
    val email      = StructField("email",    DataTypes.StringType)
    val sex        = StructField("sex",    DataTypes.StringType)

    val fields = Array(id, dept_id, name, age, email, sex)
    val employeeSchema1 = StructType(fields)


    // define a schema - approach 2
    val employeeSchema2 = new StructType()
      .add("id", LongType, false)
      .add("dept_id", LongType, false)
      .add("name", StringType, false)
      .add("age", IntegerType, false)
      .add("email", StringType, false)
      .add("sex", StringType, false)

    import spark.implicits._ // you must import implicits

//     reading data source file
    val employee = spark.read.schema(employeeSchema1).option("header", true).csv(csvFilePath).as[Employee]

    // val employee = spark.read.schema(employeeSchema2).json(jsonFilePath).as[Employee] // for JSON file

    val maleEmp45Above = employee.dropDuplicates().filter(emp => emp.age >= 45 && emp.sex == "M")

    // write

    maleEmp45Above.write.mode("overwrite").parquet("/tmp/spark/output/parquet/maleEmp45Above")
    maleEmp45Above.coalesce(1).write.option("header",true).mode("overwrite").csv("/tmp/spark/output/csv/maleEmp45Above")

  }
}
