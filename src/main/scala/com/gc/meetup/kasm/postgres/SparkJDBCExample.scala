package com.gc.meetup.kasm.postgres

import org.apache.spark.sql.SparkSession


/**
  * Created by ganeshchand on 12/7/16.
  */
case class Employee(id: Long, dept_id: Long, name: String, email: String, sex: String, age: BigInt)
object SparkJDBCExample {
  def main(args: Array[String]): Unit = {

    val jdbcUsername = "postgres"
    val jdbcPassword = "postgres"
    val jdbcHostname = "localhost"
    val jdbcPort = 5432
    val jdbcDatabase ="postgres"
    val jdbcUrl = s"jdbc:postgresql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"
    val connectionProperties = new java.util.Properties()

    // check the driver is available
    Class.forName("org.postgresql.Driver")

    // Check database connectivity
    import java.sql.DriverManager
    val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
    connection.isClosed()

    //spark

    val spark = SparkSession.builder().appName("Spark Postgres Example").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel(logLevel = "ERROR")

//    LogManager.getRootLogger.setLevel(Level.WARN)
    // read data from table
    val employee = spark.read.jdbc(jdbcUrl, "employee", connectionProperties)

//    employee.coalesce(1).write.json("/Users/ganeshchand/gh/gc/meetup/kathmandu-apache-spark-meetup-introduction-to-spark-2.0/src/main/resources/employee.json")
//    employee.coalesce(1).write.option("header", "true").csv("/Users/ganeshchand/gh/gc/meetup/kathmandu-apache-spark-meetup-introduction-to-spark-2.0/src/main/resources/employee1.csv")
//    employee.coalesce(1).write.parquet("/Users/ganeshchand/gh/gc/meetup/kathmandu-apache-spark-meetup-introduction-to-spark-2.0/src/main/resources/employee.parquet")

    import spark.implicits._

    // dataframe
    employee.groupBy($"dept_id").agg(Map("id"->"count")).sort($"count(id)".desc).show

    // sql
    employee.createOrReplaceTempView("employee")

    spark.sql(
      """
        |SELECT dept_id, count(id) FROM employee GROUP BY dept_id ORDER BY count(id) DESC """.stripMargin).show()




    // Dataset

    val employeeDS = employee.as[Employee]

    import org.apache.spark.sql.expressions.scalalang.typed.{avg => typedAvg, count => typedCount}

    val summaryByDepartment = employeeDS.groupByKey(e => e.dept_id)
      .agg(
        typedCount[Employee](_.id).name("count(id)"),
        typedAvg[Employee](_.age.toDouble).name("avg(age)"))
      .withColumnRenamed("value", "dept_id")
      .alias("Summary by color level")
    summaryByDepartment.show()

  }

}
