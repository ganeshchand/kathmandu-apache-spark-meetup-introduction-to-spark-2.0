#Apache Spark Meetup Kathmandu - Jump Start with Apache Spark 2.x 

####Clone this repository: 

```git clone https://github.com/ganeshchand/kathmandu-apache-spark-meetup-introduction-to-spark-2.0.git```

###Repl

* Download Apache Spark from [here](http://spark.apache.org/downloads.html)
* Extract files: ```tar -xf spark-2.0.2-bin-hadoop2.7.tgz```
* From Spark Home Directory: ```bin/spark-shell```
* Spark UI is available at: http://localhost:4040


```scala

//replace path/to/ with directory path of your local git repository

scala> val csvFilePath = "<path/to/>src/main/resources/employee.csv"
 

// reading input data 
scala> spark.read.csv(csvFilePath) // scheme is inferred, column names not detected
scala> spark.read.option("header", true).csv(csvFilePath).printSchema // .printSchema - all fileds are inferred as String

// define a schema
scala> import org.apache.spark.sql.types._
scala> val empSchema = new StructType()
      .add("id", LongType, false)
      .add("dept_id", LongType, false)
      .add("name", StringType, false)
      .add("age", IntegerType, false)
      .add("email", StringType, false)
      .add("sex", StringType, false) 
      
scala> spark.read.schema(empSchema).option("header", true).csv(csvFilePath).printSchema

// defining Encoder class for Employee Domain Object

scala> case class Employee(id: Long, dept_id: Long, name: String, age: Int, email: String,sex: String)


scala> val employee = spark.read.schema(empSchema).option("header", true).csv(csvFilePath).as[Employee]


scala> employee.head

scala> employee.limit(10).foreach { e => println(e)}

scala> employee.dropDuplicates.filter(_.age > 45).filter(_.sex == "M").limit(1).show

scala> val maleEmp45Above = employee.dropDuplicates().filter(emp => emp.age >= 45 && emp.sex == "M")

// query optimization
scala> employee.select($"id", $"dept_id", $"name", $"age").filter($"age" > 45).queryExecution
scala> maleEmp45Above.queryExecution.analyzed
scala> maleEmp45Above.queryExecution.optimizedPlan


// writing
scala> maleEmp45Above.write.mode("overwrite").parquet("/tmp/spark/output/parquet/maleEmp45Above")
scala> maleEmp45Above.coalesce(1).write.mode("overwrite").csv("/tmp/spark/output/csv/maleEmp45Above")
scala> maleEmp45Above.coalesce(1).write.option("header",true).mode("overwrite").csv("/tmp/spark/output/csv/maleEmp45Above") // with header


// Spark SQL

// register temporary table

scala> employee.createOrReplaceTempView("employee")
scala> spark.catalog.listTables.show
scala> spark.sql("SELECT * FROM employee")


```

###Spark JDBC

#### Pre-requisites:
* You have postgres database setup on your machine
* Create a Postgres Table

```sql

CREATE TABLE employee
(
    id BIGINT,
    dept_id BIGINT,
    name TEXT,
    email TEXT,
    age INTEGER,
    sex TEXT
);
```
* Insert test data using employee.csv file

* Make sure your have added postgres jar in the class path 

```bin/spark-shell --jars /path-to/postgresql-9.3-1101.jdbc41.jar```
* Refer ```com.gc.meetup.kasm.postgres.SparkJDBCExample.scala``` for code


###IntelliJ Project

* ```src/main/resources``` contains input data set
* ```com.gc.meetup.kasm.dataset.EmployeeDataSet.scala``` contains Dataset example
* ```com.gc.meetup.kasm.postgres``` package contains JDBC examples


###Notebook
* ```notebooks/py``` contains Databricks Pyspark notebook export (html & ipynb) 
* ```notebooks/scala``` contains Databricks Scala notebook export (html & .scala)
* ```data``` folder contains dataset used in the workshop



