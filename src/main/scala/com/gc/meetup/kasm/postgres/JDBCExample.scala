package com.gc.meetup.kasm.postgres

import java.sql.{Connection, DriverManager}

object JDBCExample {
  def main(args: Array[String]): Unit = {

      val driver = "org.postgresql.Driver"
      val url = "jdbc:postgresql://localhost/postgres"
      val username = "postgres"
      val password = "postgres"

    // simple select query
    val conn = DriverManager.getConnection(url, username, password)
    val emp = new scala.collection.mutable.MutableList[(Long,Long,String)]()
    try{
      val stmt = conn.createStatement()
      try{

        val rs = stmt.executeQuery("SELECT id, dept_id, name from employee")
        try{
          while(rs.next()){
            emp += ((rs.getLong(1), rs.getLong(2), rs.getString(3)))
          }
        }finally{
          rs.close()
        }

      }finally{
        stmt.close()
      }
    }finally{
      conn.close()
    }

    println("printing result set")
    emp.foreach(println)

    // select group by query

    var connection:Connection = null

    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("""select dept_id, count(*) as count from employee
                                               |group by dept_id ORDER BY count desc""".stripMargin)
      while ( resultSet.next() ) {
        val dept_id = resultSet.getString("dept_id")
        val count = resultSet.getString("count")
        println(s"dept_id = $dept_id, count = $count")
      }
    } catch {
      case e:Throwable => e.printStackTrace
    }
    connection.close()

  }

}