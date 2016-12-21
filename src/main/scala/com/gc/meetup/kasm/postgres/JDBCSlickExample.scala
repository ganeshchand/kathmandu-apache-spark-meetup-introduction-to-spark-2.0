package com.gc.meetup.kasm.postgres

/**
  * Created by ganeshchand on 12/11/16.
  */



// Import the Slick interface for H2:
import slick.driver.PostgresDriver.api._

import scala.concurrent.Await
import scala.concurrent.duration._

object JDBCSlickExample extends App {

  // Case class representing a row in our table:
  final case class Employee(
                            id:  Long = 0L,
                            dept_id: Long,
                            name: String,
                            email: String,
                            sex: String,
                            age: Long)

  // Helper method for creating test data:
  def freshTestData = Seq(
    Employee(31, 1,"TestUser", "testUser@gc.com", "M", 29)
  )

  // Schema for the "employee" table:
  final class EmployeeTable(tag: Tag)
    extends Table[Employee](tag, "employee") {

    def id      = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def dept_id = column[Long]("dept_id")
    def name  = column[String]("name")
    def email = column[String]("email")
    def sex = column[String]("sex")
    def age = column[Long]("age")

    def * = (id, dept_id, name, email, sex, age) <> (Employee.tupled, Employee.unapply)
  }

  // Base query for querying the employee table:
  lazy val employee = TableQuery[EmployeeTable]

  // An example query that selects a subset of employees:
  val maleEmployee = employee.filter(_.sex === "M")

  // Create a database;
  val db = Database.forConfig("demo")


  // Helper method for running a query in this example file:
  def exec[T](action: DBIO[T]): T = Await.result(db.run(action), 2 seconds)

  // Create the "employee" table:
  println("Creating database table")
//  exec(employee.schema.create) // don't run if you created a table using postgres

  // Create and insert the test data:
  println("\nInserting test data")
//  exec(employee ++= freshTestData)

  // Run the test query and print the results:
  println("\nSelecting all employees:")
  exec( employee.result ) foreach { println }



  println("\nSelecting only male employee:")
  exec( maleEmployee.result ) foreach { println }



  println("\n********Slick vs. SQL******")
  // Select all employees

  // sql
//  exec (sql"select * from employee".as[Employee]) foreach println

  // slick

//  exec(employee.result) foreach  println

  val myaction = sql"SELECT id, name, age FROM employee".as[(Long,String,Long)]
  Await.result(db.run(myaction), 2 seconds).foreach(println)
}
