import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, avg, col, count, max, min, month, sum, when, window, year}

object Problem_Questions_on_ScalaSparkDataframe {

  def main(args:Array[String]):Unit={


       val spark = SparkSession.builder()           //creating spark session
         .appName("new-adventure-calling")
         .master("local[*]")
         .getOrCreate()

    import spark.implicits._                      //calling implicit function

    //1. Student Grade Classification

    val Student_Grade_Classification = List(
      (1, "Alice", 92, "Math"),
      (2, "Bob", 85, "Math"),
      (3, "Carol", 77, "Science"),
      (4, "Dave", 65, "Science"),
      (5, "Eve", 50, "Math"),
      (6, "Frank", 82, "Science")
    ).toDF("student_id", "name", "score", "subject")

    val w1 = Window.partitionBy("subject")        //partitioning
    val df1 = Student_Grade_Classification.withColumn("grade", when(col("score")>= 90, "A")
      .when(col("score")>= 80 && col("score")< 90, "B")
      .when(col("score")>= 70 && col("score")< 80, "C")
      .when(col("score")>= 60 && col("score")< 70, "D")
      .otherwise("F"))

    val df2 = df1.withColumn("avg_score", avg(col("score")).over(w1))

    val df3 = df2.withColumn("max_score", max(col("score")).over(w1))

    val df4 = df3.withColumn("number_Of_Students", count(col("subject")).over(w1))
    df4.show()


    //2. E-commerce product analysis

    val ECommerce_Product = List(
      (1, "Smartphone", 700, "Electronics"),
      (2, "TV", 1200, "Electronics"),
      (3, "Shoes", 150, "Apparel"),
      (4, "Socks", 25, "Apparel"),
      (5, "Laptop", 800, "Electronics"),
      (6, "Jacket", 200, "Apparel")
    ).toDF("product_id", "product_name", "price", "category")

    val df5 = ECommerce_Product.withColumn("price_category", when(col("price")> 500, "Expensive")
    .when(col("price")>= 200 && col("price")<= 500, "Moderate")
    .otherwise("Cheap"))
    df5.show()


    //3. Employee Age and Salary Analysis

    val Employee_Age_and_Salary = List(
      (1, "John", 28, 60000),
      (2, "Jane", 32, 75000),
      (3, "Mike", 45, 120000),
      (4, "Alice", 55, 90000),
      (5, "Steve", 62, 110000),
      (6, "Claire", 40, 40000)
    ).toDF("employee_id", "name", "age", "salary")

    val df6 = Employee_Age_and_Salary.withColumn("age_group", when(col("age") < 30, "Young")
      .when(col("age")>= 30 && col("age")<= 50, "Mid")
      .otherwise("Senior"))

    val df7 = df6.withColumn("salary_range", when (col("salary")> 100000, "high")
    .when(col("salary")>= 50000 && col("salary")<= 100000, "medium" )
    .otherwise("low"))

    val df8 = df7.filter(col("name").startsWith("J").endsWith("e"))

    val w2 = Window.partitionBy(col("age_group"))
    val df9 = df8.withColumn("total_salary", sum(col("salary")).over(w2))
      .withColumn("avg_salary", avg(col("salary")).over(w2))
      .withColumn("min_salary", min(col("salary")).over(w2))
    df9.show()


    //4. Movie Ratings and Duration Analysis

    val  Movie_Ratings_and_Duration = List(
      (1, "The Matrix", 9, 136),
      (2, "Inception", 8, 148),
      (3, "The Godfather", 9, 175),
      (4, "Toy Story", 7, 81),
      (5, "The Shawshank Redemption", 10, 142),
      (6, "The Silence of the Lambs", 8, 118)
    )toDF("movie_id", "movie_name", "rating", "duration_minutes")

    val df11 = Movie_Ratings_and_Duration.withColumn("rating_category", when(col("rating")>= 8, "excellent")
    .when(col("rating")>= 6 && col("rating")< 10, "good")
    .otherwise("average"))
    val df12 = df11.withColumn("duration_category", when(col("duration_minutes")> 150, "Long")
      .when(col("duration_minutes") >= 90 && col("duration_minutes")<= 150, "medium")
      .otherwise("short"))
    val df13 = df12.filter(col("movie_name").startsWith("T").endsWith("e"))

    val w3 = Window.partitionBy(col("rating"))
    val df14 = df13.withColumn("total", sum(col("rating")).over(w3))
      .withColumn("avg", avg(col("rating")).over(w3))
      .withColumn("min", min(col("rating")).over(w3))
      .withColumn("max", max(col("rating")).over(w3))
    df14.show(false)

    //5. Transaction Amounts and Date Analysis

    val Transaction_Amounts_and_Date = List(
      (1, "2023-12-01", 1200, "Credit"),
      (2, "2023-11-15", 600, "Debit"),
      (3, "2023-12-20", 300, "Credit"),
      (4, "2023-10-10", 1500, "Debit"),
      (5, "2023-12-30", 250, "Credit"),
      (6, "2023-09-25", 700, "Debit")
    )
    val df15 = spark.createDataFrame(Transaction_Amounts_and_Date).toDF("transaction_id", "transaction_date", "amount", "transaction_type")
    val df16 = df15.withColumn("amount_category", when(col("amount")> 1000, "high")
      .when(col("amount")>= 500 && col("amount")<= 1000, "medium")
      .otherwise("low"))
    val df17 = df16.withColumn("transaction_month", month(col("transaction_date")))
      .filter(month($"transaction_date")===12)

    val w4 = Window.partitionBy(col("transaction_type"))
    val df18 = df17.withColumn("total", sum(col("amount")).over(w4))
      .withColumn("avg", avg(col("amount")).over(w4))
      .withColumn("min", min(col("amount")).over(w4))
      .withColumn("max", max(col("amount")).over(w4))
    df18.show()


    //6. Customer Feedback Analysis

    val Customer_Feedback = List(
      (1, "2024-01-10", 4, "Great service!"),
      (2, "2024-01-15", 5, "Excellent!"),
      (3, "2024-02-20", 2, "Poor experience."),
      (4, "2024-02-25", 3, "Good value."),
      (5, "2024-03-05", 4, "Great quality."),
      (6, "2024-03-12", 1, "Bad service.")
    )

    val df19 = spark.createDataFrame(Customer_Feedback)toDF("customer_id", "feedback_date", "rating", "feedback_text")
    val df20 = df19.withColumn("rating_category", when(col("rating")>= 5, "excellent")
      .when(col("rating")>=3 && col("rating")< 5,"good")
      .otherwise("poor"))

    val w5 = Window.partitionBy(col("feedback_date"))

    val df21 = df20.filter(col("feedback_text").startsWith("Great"))
      .withColumn("avg_rating", avg(col("rating")).over(w5))
    df21.show()

    //7. Product Sales Analysis
    val Product_Sales = List(
      (1, "Widget", 700, "2024-01-15"),
      (2, "Gadget", 150, "2024-01-20"),
      (3, "Widget", 350, "2024-02-15"),
      (4, "Device", 600, "2024-02-20"),
      (5, "Widget", 100, "2024-03-05"),
      (6, "Gadget", 500, "2024-03-12")
    )
    val df22 =spark.createDataFrame(Product_Sales)toDF("sale_id", "product_name", "sale_amount", "sale_date")
    val df23 = df22.withColumn("sale_category", when(col("sale_amount")>500, "high")
      .when(col("sale_amount")<= 200 && col("sale_amount")>= 500, "medium")
      .otherwise("LOW"))

    val w6 = Window.partitionBy(month(col("sale_date")))

    val df24 =df23.filter(col("product_name").endsWith("t"))
    val df25 = df24.withColumn("sum", sum(col("sale_amount")).over(w6))
      .withColumn("average", avg(col("sale_amount")).over(w6))
      .withColumn("min", min(col("sale_amount")).over(w6))
      .withColumn("max", max(col("sale_amount")).over(w6))
    df25.show

    //8. Employee Work Hours Analysis
    val Employee_Work_Hours = List(
      (1, "2024-01-10", 9, "Sales"),
      (2, "2024-01-11", 7, "Support"),
      (3, "2024-01-12", 8, "Sales"),
      (4, "2024-01-13", 10, "Marketing"),
      (5, "2024-01-14", 5, "Sales"),
      (6, "2024-01-15", 6, "Support")
    ).toDF("employee_id", "work_date", "hours_worked", "department")

    val df26 =Employee_Work_Hours.withColumn("hours_category", when(col("hours_worked")> 8, "overtime")
      .otherwise("regular"))

    val df27 = df26.filter(col("department").startsWith("S"))
    val w7 = Window.partitionBy("department")
    val df28 = df27.withColumn("total_hours_worked", sum(col("hours_worked")).over(w7))
      .withColumn("avg_hours_worked", avg(col("hours_worked")).over(w7))
      .withColumn("max_hours_worked", max(col("hours_worked")).over(w7))
      .withColumn("min_hours_worked", min(col("hours_worked")).over(w7))
    df28.show()

    //9. Product Inventory Analysis

    val Product_Inventory = List(
      (1, "Pro Widget", 30, "2024-01-10"),
      (2, "Pro Device", 120, "2024-01-15"),
      (3, "Standard", 200, "2024-01-20"),
      (4, "Pro Gadget", 40, "2024-02-01"),
      (5, "Standard", 60, "2024-02-10"),
      (6, "Pro Device", 90, "2024-03-01")
    )toDF("product_id", "product_name", "stock", "last_restocked")

    val df29 = Product_Inventory.withColumn("stock_status", when(col("stock")< 50, "low")
    .when(col("stock")>= 50 && col("stock")<= 150, "medium")
    .otherwise("high"))

    val df30 = df29.filter(col("product_name").contains("Pro"))
    val w8 = Window.partitionBy(month(col("last_restocked")))
    val df31 = df30.withColumn("total_stock", sum(col("stock")).over(w8))
      .withColumn("avg_stock", avg(col("stock")).over(w8))
      .withColumn("min_stock", min(col("stock")).over(w8))
      .withColumn("max_stock", max(col("stock")).over(w8))
    df31.show



    // 10. Customer Transactions Analysis
    val Customer_Transactions  = List(
      (1, 1, 1200, "2024-01-15"),
      (2, 2, 600, "2024-01-20"),
      (3, 3, 300, "2024-02-15"),
      (4, 4, 1500, "2024-02-20"),
      (5, 5, 200, "2024-03-05"),
      (6, 6, 900, "2024-03-12")
    )toDF("transaction_id", "customer_id", "transaction_amount", "transaction_date")

    val df32 = Customer_Transactions.withColumn("transaction_category", when (col("transaction_amount")> 1000, "high")
      .when(col("transaction_amount")>= 500 && col("transaction_amount")<= 1000, "medium")
      .otherwise("low"))

    val df33 = df32.filter(year(col("transaction_date"))=== 2024)
    val w9 = Window.partitionBy("transaction_category")
    val df34 = df33.withColumn("total_transaction_amount", sum(col("transaction_amount")).over(w9))
      .withColumn("avg_transaction_amount", avg(col("transaction_amount")).over(w9))
      .withColumn("max_transaction_amount", max(col("transaction_amount")).over(w9))
      .withColumn("min_transaction_amount", min(col("transaction_amount")).over(w9))
    df34.show()

    // 11. Employee Performance Review

    val Employee_Performance = List(
      (1, "2024-01-10", 8, "Good performance."),
      (2, "2024-01-15", 9, "Excellent work!"),
      (3, "2024-02-20", 6, "Needs improvement."),
      (4, "2024-02-25", 7, "Good effort."),
      (5, "2024-03-05", 10, "Outstanding!"),
      (6, "2024-03-12", 5, "Needs improvement.")
    )toDF("employee_id", "review_date", "performance_score", "review_text")

    val df35 = Employee_Performance.withColumn("performance_category", when (col("performance_score")>= 9, "excellent")
      .when(col("performance_score")>= 7 && col("performance_score")<9, "good")
      .otherwise("Needs Improvement"))

    val df36 = df35.filter(col("review_text").contains("Excellent"))
    val w11 = Window.partitionBy(month(col("review_date")))
    val df37 = df36.withColumn("avg_performance_score", avg(col("performance_score")).over(w11))
    df37.show()


    // 12. Product Rating Analysis
    val Product_Rating = List(
      (1, "Smartphone", 4, "2024-01-15"),
      (2, "Speaker", 3, "2024-01-20"),
      (3, "Smartwatch", 5, "2024-02-15"),
      (4, "Screen", 2, "2024-02-20"),
      (5, "Speakers", 4, "2024-03-05"),
      (6, "Soundbar", 3, "2024-03-12")
    )toDF("review_id", "product_name", "rating", "review_date")

    val df38 = Product_Rating.withColumn("rating_category", when(col("rating")>= 4, "high")
      .when(col("rating")>= 3 && col("rating")<4, "medium")
      .otherwise("low"))

    val df39 = df38.filter(col("product_name").startsWith("S"))
    val w12 = Window.partitionBy("rating_category")
    val df40 = df39.withColumn("review_count", count(col("rating_category")).over(w12))
      .withColumn("avg_rating", avg(col("rating")).over(w12))
    df40.show()

    // 13. Sales Performance Analysis

    val Sales_Performance = List(
      (1, "North-West", 12000, "2024-01-10"),
      (2, "South-East", 6000, "2024-01-15"),
      (3, "East-Central", 4000, "2024-02-20"),
      (4, "West", 15000, "2024-02-25"),
      (5, "North-East", 3000, "2024-03-05"),
      (6, "South-West", 7000, "2024-03-12")
    )toDF("sales_id", "region", "sales_amount", "sales_date")

    val df41 = Sales_Performance.withColumn("sales_performance", when(col("sales_amount")>10000, "excellent")
    .when(col("sales_amount")>= 5000 && col("sales_amount")<= 10000, "good")
    .otherwise("average"))

    val df42 = df41.filter(col("region").endsWith("West"))
    val w13 = Window.partitionBy("sales_performance")
    val df43 = df42.withColumn("total_sales_amount", sum(col("sales_amount")).over(w13))
      .withColumn("avg_sales_amount", avg(col("sales_amount")).over(w13))
      .withColumn("min_sales_amount", min(col("sales_amount")).over(w13))
      .withColumn("max_sales_amount", max(col("sales_amount")).over(w13))
    df43.show()

    //14. Customer Purchase History

    val Customer_Purchase = List(
      (1, 1, 2500, "2024-01-05"),
      (2, 2, 1500, "2024-01-15"),
      (3, 3, 500, "2024-02-20"),
      (4, 4, 2200, "2024-03-01"),
      (5, 5, 900, "2024-01-25"),
      (6, 6, 3000, "2024-03-12")
    )toDF("purchase_id", "customer_id", "purchase_amount", "purchase_date")

    val df44= Customer_Purchase.withColumn("purchase_category", when(col("purchase_amount")>2000, "large")
      .when(col("purchase_amount")>= 1000 && col("purchase_amount")<= 2000, "medium")
      .otherwise("small"))

    val df45 = df44.filter(month($"purchase_date") === 1 && year($"purchase_date")=== 2024)
    val w14 = Window.partitionBy("purchase_category")
    val df46 = df45.withColumn("total_purchase_amount", sum(col("purchase_amount")).over(w14))
      .withColumn("avg_purchase_amount", avg(col("purchase_amount")).over(w14))
      .withColumn("min_purchase_amount", min(col("purchase_amount")).over(w14))
      .withColumn("max_purchase_amount", max(col("purchase_amount")).over(w14))
    df46.show()

    //15. Employee Attendance Tracking

    val Employee_Attendance_Tracking = List(
      (1, "2024-01-10", 9, "Sick"),
      (2, "2024-01-11", 7, "Scheduled"),
      (3, "2024-01-12", 8, "Sick"),
      (4, "2024-01-13", 4, "Scheduled"),
      (5, "2024-01-14", 6, "Sick"),
      (6, "2024-01-15", 8, "Scheduled")
    )toDF("employee_id", "attendance_date", "hours_worked", "attendance_type")

    val df47 = Employee_Attendance_Tracking.withColumn("attendance_status", when(col("hours_worked")>= 8, "full day")
    .otherwise("Half day"))
    val df48 = df47.filter(col("attendance_type").startsWith("S"))
    val w15 =Window.partitionBy("attendance_status")
    val df49 = df48.withColumn("total_hours_worked", sum(col("hours_worked")).over(w15))
      .withColumn("avg_hours_worked", avg(col("hours_worked")).over(w15))
      .withColumn("min_hours_worked", min(col("hours_worked")).over(w15))
      .withColumn("max_hours_worked", max(col("hours_worked")).over(w15))
    df49.show()


    //16. Book Store Inventory
    val Book_Store_Inventory = List(
      (1, "The Great Gatsby", 150, "2024-01-10"),
      (2, "The Catcher in the Rye", 80, "2024-01-15"),
      (3, "Moby Dick", 200, "2024-01-20"),
      (4, "To Kill a Mockingbird", 30, "2024-02-01"),
      (5, "The Odyssey", 60, "2024-02-10"),
      (6, "War and Peace", 20, "2024-03-01")
    )

    val df50 = spark.createDataFrame(Book_Store_Inventory)toDF("book_id", "book_title", "stock_quantity", "last_updated")
    val df51 = df50.withColumn("stock_level", when(col("stock_quantity")> 100, "high")
    .when(col("stock_quantity")>= 50 && col("stock_quantity")<= 100, "medium")
    .otherwise("low"))

    val df52 = df51.filter(col("book_title").startsWith("The"))
    val w16 = Window.partitionBy("stock_level")
    val df53 = df52.withColumn("total_stock_level", sum(col("stock_quantity")).over(w16))
      .withColumn("avg_stock_level", avg(col("stock_quantity")).over(w16))
      .withColumn("min_stock_level", min(col("stock_quantity")).over(w16))
      .withColumn("max_stock_level", max(col("stock_quantity")).over(w16))
    df53.show()

    //17. Movie Theater Showtimes

    val Movie_Theater_Showtimes = List(
      (1, "Action Hero", "2024-01-10", 8),
      (2, "Comedy Nights", "2024-01-15", 25),
      (3, "Action Packed", "2024-01-20", 55),
      (4, "Romance Special", "2024-02-01", 5),
      (5, "Action Force", "2024-02-10", 45),
      (6, "Drama Series", "2024-03-01", 70)
    ).toDF("show_id", "movie_title", "showtime", "seats_available")

    val df54 = Movie_Theater_Showtimes.withColumn("availability", when(col("seats_available")<= 10, "full")
    .when(col("seats_available")>= 11 && col("seats_available")<= 50, "limited")
    .otherwise("plenty"))

    val df55 = df54.filter(col("movie_title").contains("Action"))
    val w17 = Window.partitionBy("availability")
    val df56 = df55.withColumn("total_seats_available", sum(col("seats_available")).over(w17))
      .withColumn("avg_seats_available", avg(col("seats_available")).over(w17))
      .withColumn("min_seats_available", min(col("seats_available")).over(w17))
      .withColumn("max_seats_available", max(col("seats_available")).over(w17))
    df56.show()

    //18. Employee Salary Distribution
    val Employee_Salary_Distribution = List(
      (1, "IT", 130000, "2024-01-10"),
      (2, "HR", 80000, "2024-01-15"),
      (3, "IT", 60000, "2024-02-20"),
      (4, "IT", 70000, "2024-02-25"),
      (5, "Sales", 50000, "2024-03-05"),
      (6, "IT", 90000, "2024-03-12")
    )toDF("employee_id", "department", "salary", "last_increment_date")

    val df57 = Employee_Salary_Distribution.withColumn("salary_band", when(col("salary")>120000, "high")
      .when(col("salary")>=60000 && col("salary")<= 120000, "medium")
      .otherwise("low"))

    val df58 = df57.filter(col("department").startsWith("IT"))
    val w18 = Window.partitionBy("salary_band")
    val df59 = df58.withColumn("total_salary", sum(col("salary")).over(w18))
      .withColumn("avg_salary", avg(col("salary")).over(w18))
      .withColumn("min_salary", min(col("salary")).over(w18))
      .withColumn("max_salary", max(col("salary")).over(w18))
    df59.show()

    //19. Shipment Tracking
    val Shipment_Tracking = List(
      (1, "Asia", 15000, "2024-01-10"),
      (2, "Europe", 6000, "2024-01-15"),
      (3, "Asia", 3000, "2024-02-20"),
      (4, "Asia", 20000, "2024-02-25"),
      (5, "North America", 4000, "2024-03-05"),
      (6, "Asia", 8000, "2024-03-12")
    ).toDF("shipment_id", "destination", "shipment_value", "shipment_date")

    val df60 = Shipment_Tracking.withColumn("value_category", when(col("shipment_value")> 10000, "high")
    .when(col("shipment_value")<= 5000 && col("shipment_value")>= 10000, "medium")
    .otherwise("low"))

    val df61 = df60.filter(col("destination").contains("Asia"))
    val w19 = Window.partitionBy("value_category")
    val df62 = df61.withColumn("total_shipment_value", sum(col("shipment_value")).over(w19))
      .withColumn("avg_shipment_value", avg(col("shipment_value")).over(w19))
      .withColumn("min_shipment_value", min(col("shipment_value")).over(w19))
      .withColumn("max_shipment_value", max(col("shipment_value")).over(w19))
    df62.show()


    //20.Online Purchase History
    val Online_Purchase_History = List(
      (1, 1, 700, "2024-02-05"),
      (2, 2, 150, "2024-02-10"),
      (3, 3, 400, "2024-02-15"),
      (4, 4, 600, "2024-02-20"),
      (5, 5, 250, "2024-02-25"),
      (6, 6, 1000, "2024-02-28")
    )

    val df63 = spark.createDataFrame(Online_Purchase_History)toDF("purchase_id", "customer_id", "purchase_amount", "purchase_date")
    val  df64 = df63.withColumn("purchase_status", when(col("purchase_amount")> 500, "large")
    .when(col("purchase_amount")>= 200 && col("purchase_amount")<= 500, "medium")
    .otherwise("small"))

    val df65 = df64.filter(month($"purchase_date")===2 && year($"purchase_date")===2024)
    val w20 = Window.partitionBy("purchase_status")
    val df66 = df65.withColumn("total_purchase_amount", sum(col("purchase_amount")).over(w20))
      .withColumn("avg_purchase_amount", avg(col("purchase_amount")).over(w20))
      .withColumn("min_purchase_amount", min(col("purchase_amount")).over(w20))
      .withColumn("max_purchase_amount", max(col("purchase_amount")).over(w20))
    df66.show()

    //21. Sales Target Achievement
    val Sales_Target_Achievement = List(
      (1, "John Smith", 15000, 12000),
      (2, "Jane Doe", 9000, 10000),
      (3, "John Doe", 5000, 6000),
      (4, "John Smith", 13000, 13000),
      (5, "Jane Doe", 7000, 7000),
      (6, "John Doe", 8000, 8500)
    )

    val df67 = spark.createDataFrame(Sales_Target_Achievement)toDF("sales_id", "sales_rep", "sales_amount", "target_amount")
    val df68 = df67.withColumn("achievement_status", when(col("sales_amount") >= col("target_amount"), "Above Target")
    .otherwise("Below Target"))

    val df69 = df68.filter(col("sales_rep").contains("John"))
    val w21 = Window.partitionBy("achievement_status")
    val df70 = df69.withColumn("total_sales_amount", sum(col("sales_amount")).over(w21))
      .withColumn("avg_sales_amount", avg(col("sales_amount")).over(w21))
      .withColumn("min_sales_amount", min(col("sales_amount")).over(w21))
      .withColumn("max_sales_amount", max(col("sales_amount")).over(w21))
    df70.show()

    //22. Project Budget Tracking

    val Project_Budget_Tracking = List(
      (1, "New Website", 50000, 55000),
      (2, "Old Software", 30000, 25000),
      (3, "New App", 40000, 40000),
      (4, "New Marketing", 15000, 10000),
      (5, "Old Campaign", 20000, 18000),
      (6, "New Research", 60000, 70000)
    )

    val df71 = spark.createDataFrame(Project_Budget_Tracking)toDF("project_id", "project_name", "budget", "spent_amount")
    val df72 = df71.withColumn("budget_status", when(col("spent_amount") > col("budget"), "Over Budget")
    .when(col("spent_amount")=== col("budget"), "On Budget")
      .otherwise("Under Budget"))

    val df73 = df72.filter(col("project_name").startsWith("New"))
    val w22 = Window.partitionBy("budget_status")
    val df74 = df73.withColumn("total_spent_amount", sum(col("spent_amount")).over(w22))
      .withColumn("avg_spent_amount", avg(col("spent_amount")).over(w22))
      .withColumn("max_spent_amount", max(col("spent_amount")).over(w22))
      .withColumn("min_spent_amount", min(col("spent_amount")).over(w22))
    df74.show()

    //23. Employee Bonus Calculation
    val Employee_Bonus_Calculation = List(
      (1, "Sales Department", 2500, "2024-01-10"),
      (2, "Marketing Department", 1500, "2024-01-15"),
      (3, "IT Department", 800, "2024-01-20"),
      (4, "HR Department", 1200, "2024-02-01"),
      (5, "Sales Department", 1800, "2024-02-10"),
      (6, "IT Department", 950, "2024-03-01")
    )toDF("employee_id", "department", "bonus", "bonus_date")

    val df75 = Employee_Bonus_Calculation.withColumn("bonus_category", when(col("bonus")> 2000, "High")
      .when(col("bonus") >= 1000 && col("bonus")<= 2000, "Medium")
      .otherwise("Low"))

    val df76 = df75.filter(col("department").endsWith("Department"))
    val w23 = Window.partitionBy("bonus_category")
    val df77 =df76.withColumn("total_bonus", sum(col("bonus")).over(w23))
      .withColumn("avg_bonus", avg(col("bonus")).over(w23))
      .withColumn("min_bonus", min(col("bonus")).over(w23))
      .withColumn("max_bonus", max(col("bonus")).over(w23))
    df77.show()

    //24. Customer Support Tickets
    val Customer_Support_Tickets = List(
      (1, "Bug", 1.5, "High"),
      (2, "Feature", 3.0, "Medium"),
      (3, "Bug", 4.5, "Low"),
      (4, "Bug", 2.0, "High"),
      (5, "Enhancement", 1.0, "Medium"),
      (6, "Bug", 5.0, "Low")
    )toDF("ticket_id", "issue_type", "resolution_time", "priority")

    val df78 = Customer_Support_Tickets.withColumn("resolution_status", when(col("resolution_time")<= 2, "Quick")
    .when(col("resolution_time")>2 && col("resolution_time")<= 4, "Moderate")
    .otherwise("Slow"))

    val df79 = df78.filter(col("issue_type").contains("Bug"))
    val w24 = Window.partitionBy("resolution_status")
    val df80 = df79.withColumn("total_resolution_time", sum(col("resolution_time")).over(w24))
      .withColumn("avg_resolution_time", avg(col("resolution_time")).over(w24))
      .withColumn("min_resolution_time", min(col("resolution_time")).over(w24))
      .withColumn("max_resolution_time", max(col("resolution_time")).over((w24)))
    df80.show

    // 25. Event Attendance Tracking

    val Event_Attendance_Tracking = List(
      (1, "Tech Conference", 600, "2024-01-10"),
      (2, "Sports Event", 250, "2024-01-15"),
      (3, "Tech Expo", 700, "2024-01-20"),
      (4, "Music Festival", 150, "2024-02-01"),
      (5, "Tech Seminar", 300, "2024-02-10"),
      (6, "Art Exhibition", 400, "2024-03-01")
    )toDF("event_id", "event_name", "attendees", "event_date")
    val df81 = Event_Attendance_Tracking.withColumn("attendance_status", when(col("attendees")> 500, "Full")
    .when(col("attendees")>= 200 && col("attendees")<= 500, "Moderate")
    .otherwise("Low"))

    val df82 =df81.filter(col("event_name").startsWith("Tech"))
    val w25 = Window.partitionBy("attendance_status")
    val df83 = df82.withColumn("total_attendees", sum(col("attendees")).over(w25))
      .withColumn("avg_attendance", avg(col("attendees")).over(w25))
      .withColumn("min_attendance", min(col("attendees")).over(w25))
      .withColumn("max_attendance", max(col("attendees")).over(w25))
    df83.show()


    //26. Utility Billing Analysis
    val Utility_Billing_Analysis = List(
      (1, 1, 250, "2024-02-05"),
      (2, 2, 80, "2024-02-10"),
      (3, 3, 150, "2024-02-15"),
      (4, 4, 220, "2024-02-20"),
      (5, 5, 90, "2024-02-25"),
      (6, 6, 300, "2024-02-28")
    )

    val df84 = spark.createDataFrame(Utility_Billing_Analysis)toDF("bill_id", "customer_id", "bill_amount", "billing_date")
    val df85 = df84.withColumn("bill_status", when(col("bill_amount")> 200, "High")
      .when(col("bill_amount")>= 100 && col("bill_amount")<= 200, "Medium")
      .otherwise("Low"))

    val df86 = df85.filter(month($"billing_date")=== 2 && year($"billing_date") === 2024)
    val w26 = Window.partitionBy("bill_status")
    val df87 = df86.withColumn("total_bill_amount", sum(col("bill_amount")).over(w26))
      .withColumn("avg_bill_amount", avg(col("bill_amount")).over(w26))
      .withColumn("min_bill_amount", min(col("bill_amount")).over(w26))
      .withColumn("max_bill_amount", max(col("bill_amount")).over(w26))
    df87.show()

    //27. Online Store Inventory Management
    val Online_Store_Inventory_Management = List(
      (1, "Widget Lite", 15, "2024-01-10"),
      (2, "Gadget", 60, "2024-01-15"),
      (3, "Light Lite", 25, "2024-02-20"),
      (4, "Appliance", 5, "2024-02-25"),
      (5, "Widget Pro", 70, "2024-03-05"),
      (6, "Light Pro", 45, "2024-03-12")
    )
    val df88 = spark.createDataFrame(Online_Store_Inventory_Management)toDF("product_id", "product_name", "quantity_in_stock", "last_restocked")
    val df89 = df88.withColumn("stock_status", when(col("quantity_in_stock")< 20, "critical")
    .when(col("quantity_in_stock")>= 20 && col("quantity_in_stock")< 50, "Low")
    .otherwise("Sufficient"))

    val df90 = df89.filter(col("product_name").endsWith("Lite"))
    val w27 = Window.partitionBy("stock_status")
    val df91 = df90.withColumn("total_quantity_in_stock", sum(col("quantity_in_stock")).over(w27))
      .withColumn("avg_quantity_in_stock", avg(col("quantity_in_stock")).over(w27))
      .withColumn("max_quantity_in_stock", max(col("quantity_in_stock")).over(w27))
      .withColumn("min_quantity_in_stock", min(col("quantity_in_stock")).over(w27))
    df91.show()

    //28. Employee Training Records
    val Employee_Training_Records = List(
      (1, 1, 50, "Tech"),
      (2, 2, 25, "Tech"),
      (3, 3, 15, "Management"),
      (4, 4, 35, "Tech"),
      (5, 5, 45, "Tech"),
      (6, 6, 30, "HR")
    )

    val df92 = spark.createDataFrame(Employee_Training_Records)toDF("record_id", "employee_id", "training_hours", "training_type")
    val df93 = df92.withColumn("training_status", when(col("training_hours")>40, "Extensive")
    .when(col("training_hours")>= 20 && col("training_hours")<= 40, "Moderate")
    .otherwise("Minimal"))

    val df94 = df93.filter(col("training_type").startsWith("Tech"))
    val w28 = Window.partitionBy("training_status")
    val df95 = df94.withColumn("total_training_hours", sum(col("training_hours")).over(w28))
      .withColumn("avg_training_hours", avg(col("training_hours")).over(w28))
      .withColumn("max_training_hours", max(col("training_hours")).over(w28))
      .withColumn("min_training_hours", min(col("training_hours")).over(w28))
      df95.show()

    //29. Conference Registration
    val Conference_Registration = List(
      (1, "Alice Smith", 600, "2024-03-01"),
      (2, "Bob Johnson", 350, "2024-03-05"),
      (3, "Charlie Brown", 150, "2024-03-10"),
      (4, "Dave Clark", 450, "2024-03-15"),
      (5, "Emma Wilson", 300, "2024-03-20"),
      (6, "Frank Miller", 700, "2024-03-25")
    )
    val df96 = spark.createDataFrame(Conference_Registration)toDF("registration_id", "attendee_name", "registration_fee", "registration_date")
    val df97 = df96.withColumn("fee_category", when(col("registration_fee")> 500, "premium")
      .when(col("registration_fee")>= 200 && col("registration_fee")<= 500, "standard")
      .otherwise("Basic"))
    val df98 = df97.filter(month($"registration_date")===3 && year($"registration_date") === 2024)
    val w29 = Window.partitionBy("fee_category")
    val df99 = df98.withColumn("total_registration_fee", sum(col("registration_fee")).over(w29))
      .withColumn("avg_registration_fee", avg(col("registration_fee")).over(w29))
      .withColumn("max_registration_fee", max(col("registration_fee")).over(w29))
      .withColumn("min_registration_fee", min(col("registration_fee")).over(w29))
    df99.show()

    //30.Library Book Loans
    val Library_Book_Loans = List(
      (1, "History of Rome", 40, "2024-01-10"),
      (2, "Modern History", 20, "2024-01-15"),
      (3, "Ancient History", 10, "2024-02-20"),
      (4, "European History", 15, "2024-02-25"),
      (5, "World History", 5, "2024-03-05"),
      (6, "History of Art", 35, "2024-03-12")
    )
    val df100 = spark.createDataFrame(Library_Book_Loans)toDF("loan_id", "book_title", "loan_duration_days", "loan_date")
    val df101 = df100.withColumn("loan_category", when(col("loan_duration_days")>30, "Long term")
      .when(col("loan_duration_days")>= 15 && col("loan_duration_days")<= 30, "Medium Term")
      .otherwise("Short Term"))
    val df102 = df101.filter(col("book_title").contains("History"))
    val w30 = Window.partitionBy("loan_category")
    val df103 = df102.withColumn("total_loan_duration_days", sum(col("loan_duration_days")).over(w30))
      .withColumn("avg_loan_duration_days", avg(col("loan_duration_days")).over(w30))
      .withColumn("max_loan_duration_days", max(col("loan_duration_days")).over(w30))
      .withColumn("min_loan_duration_days", min(col("loan_duration_days")).over(w30))
    df103.show()

    //31. Job Applicant Tracking
    val Job_Applicant_Tracking = List(
      (1, "Laura Green", "Hired", "2024-01-10"),
      (2, "Mark White", "Under Review", "2024-01-15"),
      (3, "John Doe", "Rejected", "2024-01-20"),
      (4, "Emily Brown", "Hired", "2024-01-25"),
      (5, "Sarah Taylor", "Under Review", "2024-01-30"),
      (6, "Chris Black", "Rejected", "2024-01-31")
    )
    val df104= spark.createDataFrame(Job_Applicant_Tracking)toDF("applicant_id", "applicant_name", "application_status", "application_date")
    val df105 = df104.withColumn("status_category", when(col("application_status")isin("Hired"), "Accepted")
     .when(col("application_status")isin("Rejected"), "Rejected")
     .otherwise("Pending"))

    val df106 = df105.filter(month($"application_date")===1 && year($"application_date")=== 2024)
    val w31 = Window.partitionBy("status_category")
    val df107 = df106.withColumn("total count", count(col("status_category")).over(w31))
    df107.show()

    //32. Employee Leave Records
    val Employee_Leave_Records = List(
      (1, "Sick", 12, "2024-01-10"),
      (2, "Sick", 7, "2024-01-15"),
      (3, "Sick", 3, "2024-02-20"),
      (4, "Sick", 6, "2024-02-25"),
      (5, "Sick", 2, "2024-03-05"),
      (6, "Casual", 5, "2024-03-12")
    )
    val df108 = spark.createDataFrame(Employee_Leave_Records)toDF("employee_id", "leave_type", "leave_duration_days", "leave_date")
    val df109 = df108.withColumn("leave_category", when(col("leave_duration_days")> 10, "Extended")
    .when(col("leave_duration_days")>= 5 && col("leave_duration_days")<= 10, "Short")
    .otherwise("Casual"))

    val df110 = df109.filter(col("leave_type")startsWith("Sick"))
    val w32 = Window.partitionBy("leave_category")
    val df111 = df110.withColumn("total_leave_duration_days", sum(col("leave_duration_days")).over(w32))
      .withColumn("avg_leave_duration_days", avg(col("leave_duration_days")).over(w32))
      .withColumn("max_leave_duration_days", max(col("leave_duration_days")).over(w32))
    df111.show()

    //33. Event Sponsorship
    val Event_Sponsorship = List(
      (1, "Alpha Corp", 12000, "2024-02-05"),
      (2, "Beta LLC", 7000, "2024-02-10"),
      (3, "Gamma Inc", 3000, "2024-02-15"),
      (4, "Delta Ltd", 9000, "2024-02-20"),
      (5, "Epsilon Co", 15000, "2024-02-25"),
      (6, "Zeta AG", 4000, "2024-02-28")
    )

    val df112 = spark.createDataFrame(Event_Sponsorship)toDF("sponsor_id", "sponsor_name", "sponsorship_amount", "sponsorship_date")
    val df113 = df112.withColumn("amount_category", when(col("sponsorship_amount")> 10000,"High")
    .when(col("sponsorship_amount")>= 5000 && col("sponsorship_amount")<= 10000, "Medium")
    .otherwise("Low"))

    val df114 = df113.filter(month($"sponsorship_date") ===2 && year($"sponsorship_date") === 2024)
    val w33 = Window.partitionBy("amount_category")
    val df115 = df114.withColumn("total_sponsorship_amount", sum(col("sponsorship_amount")).over(w33))
      .withColumn("avg_sponsorship_amount", avg(col("sponsorship_amount")).over(w33))
      .withColumn("min_sponsorship_amount", min(col("sponsorship_amount")).over(w33))
      .withColumn("max_sponsorship_amount", max(col("sponsorship_amount")).over(w33))
    df115.show()

    //34. Monthly Sales Analysis
    val Monthly_Sales = List(
      (1, "Widget", 12000, "2024-04-01"),
      (2, "Gadget", 6000, "2024-04-05"),
      (3, "Widget Pro", 3000, "2024-04-10"),
      (4, "Gadget Pro", 8000, "2024-04-15"),
      (5, "Widget Max", 15000, "2024-04-20"),
      (6, "Gadget Max", 4000, "2024-04-25")
    )
    val df116 = spark.createDataFrame(Monthly_Sales)toDF("sales_id", "product_name", "sales_amount", "sales_date")
    val df117 = df116.withColumn("sales_performance", when(col("sales_amount")> 10000, "Excellent")
    .when(col("sales_amount")>= 5000 && col("sales_amount")<= 10000, "Good" )
    .otherwise("Poor"))

    val df118 = df117.filter(month($"sales_date")===4 && year($"sales_date")===2024)
    val w34 = Window.partitionBy("sales_performance")
    val df119 = df118.withColumn("total_return_amount", sum(col("sales_amount")).over(w34))
      .withColumn("avg_sales_amount", avg(col("sales_amount")).over(w34))
      .withColumn("max_sales_amount", max(col("sales_amount")).over(w34))
      .withColumn("min_sales_amount", min(col("sales_amount")).over(w34))
    df119.show()




    //35. Product Return Analysis

    val Product_Return  = List(
      (1, "Widget Pro", 6000, "2024-05-01"),
      (2, "Gadget Pro", 3000, "2024-05-05"),
      (3, "Widget Max", 1500, "2024-05-10"),
      (4, "Gadget Max", 2500, "2024-05-15"),
      (5, "Widget Pro", 7000, "2024-05-20"),
      (6, "Gadget Max", 1000, "2024-05-25")
    )

    val df120 = spark.createDataFrame(Product_Return)toDF("return_id", "product_name", "return_amount", "return_date")
    val df121 = df120.withColumn("return_status", when(col("return_amount")> 5000, "High")
      .when(col("return_amount")>= 2000 && col("return_amount")<= 5000, "Medium" )
      .otherwise("Low"))

    val df122 = df121.filter(col("product_name").endsWith("Pro"))
    val w35 = Window.partitionBy("return_status")
    val df123 = df122.withColumn("total_return_amount", sum(col("return_amount")).over(w35))
      .withColumn("avg_return_amount", avg(col("return_amount")).over(w35))
      .withColumn("max_return_amount", max(col("return_amount")).over(w35))
      .withColumn("min_return_amount", min(col("return_amount")).over(w35))
    df123.show()

    //36. Supplier Transactions
    val Supplier_Transactions = List(
      (1, "Alpha Ltd", 16000, "2024-06-01"),
      (2, "Beta Inc", 8000, "2024-06-05"),
      (3, "Gamma LLC", 4000, "2024-06-10"),
      (4, "Delta Co", 12000, "2024-06-15"),
      (5, "Epsilon Ltd", 18000, "2024-06-20"),
      (6, "Zeta Corp", 3000, "2024-06-25")
    )

    val df124 = spark.createDataFrame(Supplier_Transactions)toDF("transaction_id", "supplier_name", "transaction_amount", "transaction_date")
    val df125 = df124.withColumn("transaction_status", when(col("transaction_amount") > 15000, "High")
    .when(col("transaction_amount") >= 5000 && col("transaction_amount")<= 15000, "Medium")
    .otherwise("Low"))

    val df126 = df125.filter(month($"transaction_date") === 6 && year($"transaction_date")=== 2024)
    val w36 = Window.partitionBy("transaction_status")
    val df127 = df126.withColumn("total_transaction_status", sum(col("transaction_amount")).over(w36))
      .withColumn("avg_transaction_amount", avg(col("transaction_amount")).over(w36))
      .withColumn("max_transaction_amount", max(col("transaction_amount")).over(w36))
      .withColumn("min_transaction_amount", min(col("transaction_amount")).over(w36))
    df127.show()


    //37. Corporate Training Expenses

    val Corporate_Training_Expenses = List (
      (1, "HR", 3500, "2024-07-01"),
      (2, "IT", 1200, "2024-07-05"),
      (3, "HR", 600, "2024-07-10"),
      (4, "HR", 2500, "2024-07-15"),
      (5, "IT", 800, "2024-07-20"),
      (6, "HR", 4000, "2024-07-25")
    )
    val df128 = spark.createDataFrame(Corporate_Training_Expenses)toDF("expense_id", "department", "expense_amount", "expense_date")
    val df129 = df128.withColumn("expense_category", when(col("expense_amount")>3000, "High")
    .when(col("expense_amount")>= 1000 && col("expense_amount")<= 3000, "Medium")
    .otherwise("Low"))

    val df130 = df129.filter(col("department").startsWith("HR"))
    val w37 = Window.partitionBy("expense_category")
    val df131 = df130.withColumn("total_expense_amount", sum(col("expense_amount")).over(w37))
      .withColumn("avg_expense_amount", avg(col("expense_amount")).over(w37))
      .withColumn("max_expense_amount", max(col("expense_amount")).over(w37))
      .withColumn("min_expense_amount", min(col("expense_amount")).over(w37))
    df131.show()

    //38. Customer Purchase History

    val Customer_Purchase_History = List(
      (1, 1, 2500, "2024-08-01"),
      (2, 2, 1500, "2024-08-05"),
      (3, 3, 800, "2024-08-10"),
      (4, 4, 3000, "2024-08-15"),
      (5, 5, 1200, "2024-08-20"),
      (6, 6, 5000, "2024-08-25")
    )
    val df132 = spark.createDataFrame(Customer_Purchase_History)toDF("purchase_id", "customer_id", "purchase_amount", "purchase_date")
    val df133 =df132.withColumn("purchase_level", when(col("purchase_amount")>2000, "High")
      .when(col("purchase_amount")>= 1000 && col("purchase_amount")<= 2000, "Medium")
      .otherwise("low"))

    val df134 = df133.filter(month($"purchase_date") ===8 && year($"purchase_date")=== 2024)
    val w38 = Window.partitionBy("purchase_level")
    val df135 = df134.withColumn("total_purchase_amount", sum(col("purchase_amount")).over(w38))
      .withColumn("avg_purchase_amount", avg(col("purchase_amount")).over(w38))
      .withColumn("max_purchase_amount", max(col("purchase_amount")).over(w38))
      .withColumn("min_purchase_amount", min(col("purchase_amount")).over(w38))
    df135.show()

    //39. Project Expense Tracking
    val Project_Expense_Tracking = List(
      (1, "Development Project", 8000, "2024-09-01"),
      (2, "Development Plan", 4500, "2024-09-05"),
      (3, "Marketing Campaign", 2500, "2024-09-10"),
      (4, "Development Phase", 3000, "2024-09-15"),
      (5, "Development Task", 10000, "2024-09-20"),
      (6, "R&D Project", 1500, "2024-09-25")
    )
    val df136 = spark.createDataFrame(Project_Expense_Tracking)toDF("expense_id", "project_name", "expense_amount", "expense_date")
    val df137 = df136.withColumn("expense_type", when(col("expense_amount")> 7000, "High")
    .when(col("expense_amount")>= 3000 && col("expense_amount")<= 7000, "Medium")
    .otherwise("Low"))

    val df138 = df137.filter(col("project_name").contains("Development"))
    val w39 = Window.partitionBy("expense_type")
    val df139 = df138.withColumn("total_expense_amount", sum(col("expense_amount")).over(w39))
      .withColumn("avg_expense_amount", avg(col("expense_amount")).over(w39))
      .withColumn("max_expense_amount", max(col("expense_amount")).over(w39))
      .withColumn("min_expense_amount", min(col("expense_amount")).over(w39))
    df139.show()

    //40. Employee Performance Ratings
    val Employee_performance_Ratings = List(
      (1, 95, "2024-10-01"),
      (2, 85, "2024-10-05"),
      (3, 65, "2024-10-10"),
      (4, 75, "2024-10-15"),
      (5, 90, "2024-10-20"),
      (6, 80, "2024-10-25")
    )
    val df140 = spark.createDataFrame(Employee_performance_Ratings)toDF("employee_id", "rating_score", "rating_date")
    val df141 = df140.withColumn("rating_category", when(col("rating_score") >= 90, "Excellent")
    .when(col("rating_score")>= 70 && col("rating_score")< 90, "Good")
    otherwise("Needs Improvement"))

    val df142 = df141.filter(month($"rating_date")=== 10 && year($"rating_date") === 2024)
    val w40 =Window.partitionBy("rating_category")
    val df143 = df142.withColumn("count of each rating category", count(col("rating_category")).over(w40))
    df143.show()









  }

}
