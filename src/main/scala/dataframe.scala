import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, dayofmonth, dayofyear, month, when}

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
      df11.show(false)


    //question twelve
    val reviews = List(
      (1, 1),
      (2, 4),
      (3, 5)
    ).toDF("review_id", "rating")
    val df12 = reviews.withColumn("rating_comment", when (col("rating")<3 , "bad")
      .when(col("rating")===3 || col("rating")===4, "good")
      .otherwise("excellent"))
    val df13 = df12.withColumn("is_positive", when (col("rating")>= 3, "True")
      .otherwise("false"))
    df13.show()

    //question thirteen
    val documents = List(
      (1, "The quick brown fox"),
      (2, "Lorem ipsum dolor sit amet"),
      (3, "Spark is unified analytics engine")
    ).toDF("doc_id", "content")
    val df14  = documents.withColumn("content_category", when(col("content").contains("fox"), "Animal Related")
      .when(col("content").contains("Lorem"), "Placeholder Text")
      .when(col("content").contains("Spark"), "Tech Related"))
    df14.show(false)

    //question fourteen
    val tasks = List(
      (1, "2024-07-01", "2024-07-10"),
      (2, "2024-08-01", "2024-08-15"),
      (3, "2024-09-01", "2024-09-05")
    ).toDF("task_id", "start_date", "end_date")
    val df15 = tasks.withColumn("task_duration", when(col("end_date").minus(col("start_date"))<7, "short")
      .when(col("end_date").minus(col("start_date"))>= 7 && col("end_date").minus(col("start_date"))<= 14, "Medium")
      .otherwise("Long"))
    df15.show()

    //question fifteen
    val orders_ = List(
      (1, 5, 100),
      (2, 10, 150),
      (3, 20, 300)
    ).toDF("order_id", "quantity", "total_price")
    val df16 = orders_.withColumn("order_type", when(col("quantity")< 10 && col("total_price")< 200, "Small & Cheap")
      .when(col("quantity")>= 10 && col("total_price") < 200, "Bulk & Discounted" )
      .otherwise("Premium Order"))
    df16.show()

    //question sixteen
    val weather = Seq(
      (1, 25, 60),
      (2, 35, 40),
      (3, 15, 80)
    ).toDF("day_id", "temperature", "humidity")
    val  df17 = weather.withColumn("is_hot", when(col("temperature")> 30, "true")
    .otherwise("false"))
    val df18 = df17.withColumn("is_humid", when(col("humidity") > 35, "true")
      .otherwise("false"))
    df18.show()

    //question seventeen
    val scores = Seq (
      (1, 85, 92),
      (2, 58, 76),
      (3, 72, 64)
    ).toDF("student_id", "math_score", "english_score")
    val df19 = scores.withColumn("math_grade", when(col("math_score")>80, "A")
      .when(col("math_score")>60 &&  col("math_score") < 80, "B")
      .otherwise("C"))
    val df20 = df19.withColumn("english_grade", when(col("english_score")> 80, "A")
      .when(col("english_score")> 60 && col("english_score")< 80, "B")
      .otherwise("C"))
    df20.show()

    //question eighteen
    val emails = List(
      (1, "user@gmail.com"),
      (2, "admin@yyahoo.com"),
      (3, "info@hotmail.com")
    ).toDF("email_id", "email_address")
    val df21 = emails.withColumn("email_domain", when (col("email_address").contains("gmail"), "Gmail")
      .when(col("email_address").contains("yahoo"), "Yahoo")
      .otherwise("Hotmail"))
    df21.show()

    //question nineteen
    val payments = List(
      (1, "2024-07-15"),
      (2, "2024-12-25"),
      (3, "2024-11-01")
    ).toDF("payment_id", "payment_date")
    val df22= payments.select(col("payment_id"), col("payment_date"),
      when(month(col("payment_date")).isin(1,2,3) , "Q1")
        .when(month(col("payment_date")).isin(4,5,6), "Q2")
        .when(month(col("payment_date")).isin(7,8,9), "Q3")
        .otherwise("Q4")
        .alias("quarter"))
      df22.show()


  }
}
