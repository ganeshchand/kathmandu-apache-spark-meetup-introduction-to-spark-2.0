#Apache Spark Meetup Kathmandu - Jump Start with Apache Spark 2.x 

####Clone this repository: ```git clone https://github.com/ganeshchand/kathmandu-apache-spark-meetup-introduction-to-spark-2.0.git```

###Repl

* Download Apache Spark from [here](http://spark.apache.org/downloads.html)
* Extract files: ```tar -xf spark-2.0.2-bin-hadoop2.7.tgz```
* From Spark Home Directory: ```bin/spark-shell```
* Spark UI is available at: http://localhost:4040

```scala
import org.apache.spark.sql.types._

val csvFilePath = "<path/to/>src/main/resources/employee.csv"
val jsonFilePath = "<path/to/>src/main/resources/employee.json"
val parquetFilePath = "<path/to/>src/main/resources/employee.parquet"

//replace path/to/ with directory path of your local git repository 

// reading input data files 
spark.read.csv(csvFilePath) // scheme is inferred, column names not detected
spark.read.option("header", true).csv(csvFilePath).printSchema // .printSchema - all fileds are inferred as String

// define a schema

      val empSchema = new StructType()
      .add("id", LongType, false)
      .add("dept_id", LongType, false)
      .add("name", StringType, false)
      .add("age", IntegerType, false)
      .add("email", StringType, false)
      .add("sex", StringType, false) 
      
spark.read.schema(empSchema).option("header", true).csv(csvFilePath).printSchema

// defining Encoder class for Employee Domain Object

case class Employee(id: Long, dept_id: Long, name: String, age: Int, email: String,sex: String)

spark.read.schema(employeeSchema2).option("header", true).csv(csvFilePath).as[Employee]

val employee = spark.read.schema(employeeSchema2).option("header", true).csv(csvFilePath).as[Employee]


employee.head

employee.limit(10).foreach { e => println(e)}

employee.dropDuplicates.filter(_.age > 45).filter(_.sex == "M").limit(1).show

val maleEmp45Above = employee.dropDuplicates().filter(emp => emp.age >= 45 && emp.sex == "M")

// query optimization
employee.select($"id", $"dept_id", $"name", $"age").filter($"age" > 45).queryExecution
maleEmp45Above.queryExecution.analyzed
maleEmp45Above.queryExecution.optimizedPlan


// writing
maleEmp45Above.write.mode("overwrite").parquet("/tmp/spark/output/parquet/maleEmp45Above")
maleEmp45Above.coalesce(1).write.mode("overwrite").csv("/tmp/spark/output/csv/maleEmp45Above")
maleEmp45Above.coalesce(1).write.option("header",true).mode("overwrite").csv("/tmp/spark/output/csv/maleEmp45Above") // with header


```

###JDBC - READ and WRITE 

#### Pre-requisites:
* You have postgres database setup on your machine
* Create a Postgres Table
```sql

CREATE TABLE employee1
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

```bin/spark-shell --jars /Users/ganeshchand/bin/jars/postgresql-9.3-1101.jdbc41.jar```


###IntelliJ Project

* src/main/resources contains input data set
* com.gc.meetup.kasm.dataset.EmployeeDataSet.scala contains Dataset example
* com.gc.meetup.kasm.postgres package contains JDBC examples



