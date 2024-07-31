import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, month, when}

object dataframeAssignment {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("sparkprogramOla")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //question one
    val employees = List(
      (1, "john", 28),
      (2, "jane", 35),
      (3, "Doe", 22)
    ).toDF("id", "name", "age").toDF("id", "name", "age")

    val df1=employees.withColumn("adult",when(col("age")>18,"True").otherwise("false"))
    df1.show()

    //question two
    val grades = List(
      (1,85),
      (2,42),
      (3,73)
    ).toDF("student_id","score")
    val df2=grades.withColumn("grade", when (col("score")>= 50, "pass").otherwise("fail"))
    df2.show()

    //question three
    val transactions = List(
      (1, 1000),
      (2,200),
      (3,5000)
    ).toDF("transaction_id","amount")
    val df3 = transactions.withColumn("category", when (col("amount")> 1000, "high")
      .when(col("amount")>= 500 && col("amount")<= 1000, "medium")
      .otherwise("low"))
      df3.show()

    //question four
    val products= List(
      (1, 30.5),
      (2, 150.75),
      (3, 75.25)
    ).toDF("product_id", "price")
    val df4 = products.withColumn("price_range", when (col("price")< 50, "cheap")
      .when(col("price")>= 50 && col ("price") <= 100 , "moderate")
      .otherwise("expensive"))
      df4.show

    //question five
    val events = List(
      (1, "2024-07-27"),
      (2, "2024-12-25"),
      (3, "2025-01-01")
    ).toDF("event_id", "date")
    val df5 = events.withColumn("is_holiday", when (col("date") === "2024-12-25" || col("date") === "2025-01-01", "true")
      .otherwise("false"))
    df5.show()

    //question six
    val inventory = List(
      (1,5),
      (2,15),
      (3,25)
    ).toDF("item", "quantity")
    val df6 = inventory.withColumn("stock_level", when(col("quantity")<10, "low")
      .when(col("quantity")>=10 && col("quantity")<= 20, "medium")
      .otherwise("High"))
      df6.show()

    //question seven
    val customers = List(
      (1, "John@gmail.com"),
      (2, "Jane@yahoo.com"),
      (3, "doe@hotmail.com")
    ).toDF("customer_id", "email")
    val df7 = customers.withColumn("email_provider", when(col("email").contains("gmail"), "Gmail")
      .when(col("email").contains("yahoo"), "Yahoo")
      .otherwise("other"))
      df7.show()

    //question eight
    val orders = List(
      (1, "2024-07-01"),
      (2, "2024-12-01"),
      (3, "2024-05-01")
    ).toDF("order_id", "order_date")
    val df8 = orders.withColumn("season", when(month(col("order_date")).isin(6,7,8), "summer")
      .when(month(col("order_date")).isin(12,1,2), "winter")
      .otherwise("other"))
      df8.show()

    //question nine
    val sales = List(
      (1,100),
      (2, 1500),
      (3, 300)
    ).toDF("sale_id", "amount")
    val df9 = sales.withColumn("discount", when(col("amount")<200, 0)
    .when(col("amount")>200 && col("amount")<1000, 10)
      .otherwise(20))
    df9.show()

    //question ten
    val logins = List(
      (1, "09:00"),
      (2, "18:30"),
      (3, "14:00")
    ).toDF("login_id", "login_time")
    val df10 = logins.withColumn("is_morning", when(col("login_time").lt("12:00:00"), "true")
      .otherwise("false"))
      df10.show()

    //question eveven
    val employees_ = List(
      (1, 25, 30000),
      (2, 45, 50000),
      (3, 35, 40000)
    ).toDF("employee_id", "age", "salary")
    val df11 = employees_.withColumn("values", when(col("age")< 30 && col("salary")<35000, "young & low salary")
      .when(col("age") < 30 && col ("salary")<35000, "young & low salary")
      .when(col("age")>= 30 && col("age")<= 40 && col("salary")>= 35000 && col ("salary") <= 45000, "middle aged and medium salary")
      .otherwise("old & high salary"))
      df11.show()

    

  }
}