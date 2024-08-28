import org.apache.spark.sql.catalyst.ScalaReflection.universe.show
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{abs, avg, col, count, date_format, datediff, from_unixtime, lag, lead, max, min, month, regexp_replace, split, sum, to_date, to_timestamp, unix_timestamp, when, year}


object twentyfive_scalaSparkDataFrame_problems {
  def main(args:Array[String]):Unit={

    val jideBritish = SparkSession.builder()
      .appName("twenty_five")
      .master("local[*]")
      .getOrCreate()


    import jideBritish.implicits._


    //1. Employee Performance Analysis
    val Employee_Performance = List(
      ("E001", "Sales", 85, "2024-02-10", "Sales Manager"),
      ("E002", "HR", 78, "2024-03-15", "HR Assistant"),
      ("E003", "IT", 92, "2024-01-22", "IT Manager"),
      ("E004", "Sales", 88, "2024-02-18", "Sales Manager"),
      ("E005", "HR", 95, "2024-03-20", "HR Manager")
    )
    val df1 = jideBritish.createDataFrame(Employee_Performance)toDF("employee_id", "department", "performance_score", "review_date", "position")
    val df2 = df1.withColumn("review_month", month($"review_date"))
    val df3 = df2.filter(col("position").endsWith("Manager") && col("performance_score")> 80)
    val w1 = Window.orderBy("employee_id")
    val df4 = df3.withColumn("lagged", lag(col("performance_score"), 1).over(w1))
    val df555 = df4.withColumn("performance improvement or decline", col("performance_score")- col("lagged"))
    val df666 = df555.groupBy("department", "review_month")
      .agg(
        avg("performance_score").as("average performance score")
        , sum(when($"performance_score" > 90, 1).otherwise(0)).as("count of employees")
      )
    df666.show()


//    2. Customer Churn Analysis
    val Customer_Churn = List(
      ("C001", "Premium Gold", "Yes", "2023-12-01", 1200, "USA"),
      ("C002", "Basic", "No", "NULL", 400, "Canada"),
      ("C003", "Premium Silver", "Yes", "2023-11-15", 800, "UK"),
      ("C004", "Premium Gold", "Yes", "2024-01-10", 1500, "USA"),
      ("C005", "Basic", "No", "NULL", 300, "India")
    )

    val df777 = jideBritish.createDataFrame(Customer_Churn)toDF("customer_id", "subscription_type", "churn_status", "churn_date", "revenue", "country")
    val df888 = df777.withColumn("churn_year" , year($"churn_date"))
    val w2 = Window.partitionBy("country").orderBy("customer_id")
    val df999 = df888.withColumn("lead", lead(col("revenue"), 1).over(w2))
    val df1000 = df999.filter(col("subscription_type").startsWith("Premium") && col("churn_status").isNotNull)
    val df1111 = df1000.groupBy("country", "churn_year")
      .agg(
        sum("revenue").as("total revenue lost due to churn")
        ,avg("revenue").as("avg revenue per churned customer")
        ,count("revenue").as("count of churned customers")
      )
    df1111.show()


    //3. Sales Target Achievement Analysis
    val Sales_Target_Achievement = List(
      ("S001", 15000, 12000, "2023-12-10", "North", "Electronics Accessories"),
      ("S002", 8000, 9000, "2023-12-11", "South", "Home Appliances"),
      ("S003", 20000, 18000, "2023-12-12", "East", "Electronics Gadgets"),
      ("S004", 10000, 15000, "2023-12-13", "West", "Electronics Accessories"),
      ("S005", 18000, 15000, "2023-12-14", "North", "Furniture Accessories"),
      ("S006", 22000, 20000, "2023-12-15", "North", "Electronics Accessories"),
      ("S005", 18000, 15000, "2023-12-14", "North", "Furniture Accessories")
    )
    val df1200 = jideBritish.createDataFrame(Sales_Target_Achievement)toDF("salesperson_id", "sales_amount", "target_amount", "sale_date", "region", "product_category")
    val df1300 = df1200.withColumn("target_achieved", when(col("sales_amount")>=col("target_amount"), "yes")
      .otherwise("No"))

    val w3 = Window.orderBy("region")

    val df1400 = df1300.withColumn("lag of sales amount", lag("sales_amount", 1).over(w3))
    val df1500 = df1400.filter(col("product_category").startsWith("Electronics"))
    val df1600 = df1500.filter(col("product_category").endsWith("Accessories"))
    df1600.show()
    val df1700 = df1600.groupBy("region", "product_category")
      .agg(
        sum("sales_amount").as("total sales_amount")
        ,min("sales_amount").as("minimum sales_amount")
        ,sum(when(col("target_achieved")isin("yes"),1).otherwise(0)).as("nos of salespersons achieved targeet")
      )
    df1700.show(false)

    //4. Loan Repayment Analysis
    val Loan_Repayment = List(
      ("L001", "C001", 1000, "2023-11-01", "2023-12-01", "Personal Loan", "7.5%"),
    ("L002", "C002", 2000, "2023-11-15", "2023-11-20", "Home Loan", "6.5%"),
      ("L003", "C003", 1500, "2023-12-01", "2023-12-25", "Personal Loan", "8.0%"),
    ("L004", "C004", 2500, "2023-12-10", "2024-01-15", "Car Loan", "9.0%"),
    ("L005", "C005", 1200, "2023-12-15", "2024-01-20", "Personal Loan", "7.5%")
    )
    val df1800 = jideBritish.createDataFrame(Loan_Repayment)toDF("loan_id", "customer_id", "repayment_amount", "due_date", "payment_date", "loan_type", "interest_rate")
    val cleanedDF = df1800.withColumn("interest_rate_col", regexp_replace($"interest_rate", "%", "").cast("double")/100)
    val df1900 = cleanedDF.withColumn("repayment_delay", datediff(col("payment_date"), col("due_date")))
    val  w4 = Window.orderBy("customer_id")
    val df2000 = df1900.withColumn("lead value", lead("repayment_amount", 1).over(w4))
    val df2100 = df2000.filter(col("loan_type").startsWith("Personal") && col("repayment_delay")>=4)
    val df2200 = df2100.groupBy("loan_type", "interest_rate_col")
      .agg(
        sum("repayment_amount").as("total repayment amount")
        ,max("repayment_delay").as("max repayment delay")
        ,avg("interest_rate_col").as("avg of interest"))
    df2200.show()

    //5. Website Traffic Analysis
    val Website_Traffic = List(
    ("S001", "U001", 15, "2023-10-01", "Mobile", "Organic"),
    ("S002", "U002", 10, "2023-10-05", "Desktop", "Paid"),
    ("S003", "U003", 20, "2023-10-10", "Mobile", "Organic"),
    ("S004", "U004", 25, "2023-10-15", "Tablet", "Referral"),
    ("S005", "U001", 30, "2023-11-01", "Mobile", "Organic")
    )
    val df5 = jideBritish.createDataFrame(Website_Traffic)toDF("session_id", "user_id", "page_views", "session_date", "device_type", "traffic_source")
    val df6 = df5.withColumn("session_month", month($"session_date"))
    val w5 =Window.orderBy("user_id")
    val df7 = df6.withColumn("lag_value", lag("page_views",1).over(w5))
    val df8 = df7.withColumn("diff in consecutive pages", col("page_views")-col("lag_value")).drop("lag_value")
    val df9 = df8.filter(col("traffic_source").isin("Organic") && col("device_type").isin("Mobile"))
    val df10 = df9.groupBy("device_type", "session_month")
      .agg(
        sum("page_views").as("total page_views")
        ,avg("page_views").as("avg page_views")
        ,count("session_id").as("count of sessions")
      )
    df10.show()

    //6. Product Return Analysis
    val Product_Return = List(
      ("R001", "O001", "Electro Gadgets", "2023-12-01", "Damaged", 100),
      ("R002", "O002", "Home Appliances", "2023-12-05", "Defective", 50),
      ("R003", "O003", "Electro Toys", "2023-12-10", "Changed Mind", 75),
    ("R004", "O004", "Electro Gadgets", "2023-12-15", "Damaged", 100),
    ("R005", "0005", "Kitchen Set", "2023-12-20", "Wrong Product", 120)
    )
    val df11 = jideBritish.createDataFrame(Product_Return)toDF("return_id", "order_id", "product_name", "return_date", "return_reason", "refund_amount")
    val df12 = df11.withColumn("return_year", year($"return_date"))
    val w6 = Window.orderBy("return_id")
    val df13 = df12.withColumn("lag values", lag("refund_amount", 1).over(w6))
    val df14 = df13.withColumn("refund amount diff", col("refund_amount")-col("lag values")).drop("lag values")
    val df15 = df14.filter(col("product_name").startsWith("Electro"))
    val df16 = df15.groupBy("return_reason", "return_year")
      .agg(
        sum("refund_amount").as("total refund amount")
        ,count("return_reason").as("count of returns")
      )
    df16.show()


    //7. Hospital Patient Visit Analysis
    val Hospital_Patient_Visit = List(
      ("V001", "P001", "2023-11-01", "Dr. Smith", 700, "Cardiology"),
      ("V002", "P002", "2023-11-10", "Dr. Johnson", 400, "Neurology"),
      ("V003", "P003", "2023-12-01", "Dr. Brown", 900, "Cardiology"),
      ("V004", "P004", "2023-12-15", "Dr. Smith", 600, "Cardiology"),
      ("V005", "P005", "2024-01-01", "Dr. Johnson", 450, "Neurology")
    )
    val df17 = jideBritish.createDataFrame(Hospital_Patient_Visit)toDF("visit_id", "patient_id", "visit_date", "doctor_name", "treatment_cost", "department")
    val df18 = df17.withColumn("visit_year", year($"visit_date"))
    val w7 = Window.orderBy("patient_id")
    val df19 = df18.withColumn("next visit treatment cost", lead("treatment_cost", 1).over(w7))
    val df20 = df19.filter(col("department").startsWith("Cardio") && col("treatment_cost")> 500)
    val df21 = df20.groupBy("doctor_name", "visit_year")
      .agg(
        sum("treatment_cost").as("total treatment cost")
        ,max("treatment_cost").as("max treatment cost")
        ,count("doctor_name").as("count of visit")
      )
    df21.show()

    //8. Product Price Fluctuation Analysis
    val Product_Price_Fluctuation = List(
      ("P001", "Mobile Phone", "2023-10-01", 500, "Electronics"),
      ("P002", "Washing Machine", "2023-10-05", 700, "Home Appliances"),
      ("P003", "Laptop", "2023-10-10", 1200, "Electronics"),
      ("P004", "TV", "2023-10-15", 1300, "Consumer Electronics"),
      ("P005", "Laptop", "2023-10-10", 1700, "Electronics"),
      ("P006", "Mobile Phone", "2023-11-01", 1900, "Electronics")
    )
    val df22 = jideBritish.createDataFrame(Product_Price_Fluctuation)toDF("product_id", "product_name", "price_date", "price", "category")
    val df23 = df22.withColumn("price_month", month($"price_date"))
    val w8 = Window.orderBy("product_id")
    val df24 = df23.withColumn("lagged value", lag("price", 1).over(w8))
    val df25 = df24.withColumn("%change in price", (col("price")-col("lagged value"))/100).drop("lagged value")
    val df26 = df25.filter(col("category").endsWith("Electronics") && col("price")> col("lagged value"))
    val df27 = df26.groupBy("product_name", "price_month")
      .agg(
        avg("price").as("avg price")
        , max("price")-min("price").as("diff betw max min price")
        , count("price").as("count of price changes")
      )
    df27.show()

    //9, Employee attendance analysis
    val Employee_attendance = List(
      ("A001", "E001", "2023-11-01", "Present", "Sales", "Day"),
      ("A002", "E002", "2023-11-02", "Absent", "HR", "Night"),
      ("A003", "E003", "2023-11-03", "Present", "IT", "Night"),
      ("A004", "E001", "2023-11-04", "Absent", "Sales", "Night"),
      ("A005", "E002", "2023-11-05", "Present", "HR", "Day"),
      ("A006", "E002", "2023-11-06", "Absent", "HR", "Night")
    )

    val  df28 = jideBritish.createDataFrame(Employee_attendance)toDF("attendance_id", "employee_id", "attendance_date", "status", "department", "shift")
    val df29 = df28.withColumn("attendance_month", month($"attendance_date"))
    val w9 = Window.orderBy("employee_id")
    val df30 = df29.withColumn("predicted next attendance status", lead("status", 1).over(w9))
    val df31 = df30.filter(col("status").isin("Absent") || col("shift").contains("Night"))
    val df32 = df31.groupBy("department", "attendance_month")
      .agg(
        sum(when(col("status").contains("Absent"),1).otherwise(0).as("number of absence")).as("nos of days absent")
          , avg(when(col("status").contains("Absent"), 1).otherwise(0)).as("avg nos of days absent")
//        , max(col(""))
      )
    df32.show()


    //10.  Airline Flight Delay Analysis
    val Airline_Flight_Delay = List(
      ("F001", "Delta", "2023-11-01", "08:00", "2023-11-01", "10:00", 40, "New York"),
    ("F002", "United", "2023-11-01", "09:00", "2023-11-01", "11:30", 20, "New Orleans"),
    ("F003", "American", "2023-11-02", "07:30", "2023-11-02", "09:00", 60, "New York"),
    ("F004", "Delta", "2023-11-02", "10:00", "2023-11-02", "12:15", 30, "Chicago"),
     ("F005", "United", "2023-11-03", "08:45", "2023-11-03", "11:00", 50, "New York")
    )
    val df35 = jideBritish.createDataFrame(Airline_Flight_Delay)toDF("flight_id", "airline","departure_date","departure_time","arrival_date", "arrival_time", "delay", "destination")
    val df35_5 = df35.withColumn("departure_time2", to_timestamp($"departure_time"))
                     .withColumn("arrival_time2", to_timestamp($"arrival_time")).drop("departure_time","arrival_time")
    val  df36 = df35_5.withColumn("delay_minutes", abs(unix_timestamp($"departure_time2")-unix_timestamp($"arrival_time2"))/60)
    val df37 = df36.filter(col("delay_minutes")> 30 && col("destination").startsWith("New"))
    val w10 = Window.partitionBy("airline").orderBy("flight_id")
    val lagstuff = df37.withColumn("delay trend", lag("delay",1).over(w10))
    val df38 = lagstuff.groupBy("airline","destination")
      .agg(
      sum("delay_minutes").as("total delay minute")
      ,max("delay_minutes").as("max delay minute")
      ,avg("delay_minutes").as("avg delay mins")
    )
    df38.show()








//    val Airline_Flight_Delay2 = List(
//      ("F001", "Delta", "2023-11-01 08:00", "2023-11-01 10:00", 40, "New York"),
//      ("F002", "United", "2023-11-01 09:00", "2023-11-01 11:30", 20, "New Orleans"),
//      ("F003", "American", "2023-11-02 07:30", "2023-11-02 09:00", 60, "New York"),
//      ("F004", "Delta", "2023-11-02 10:00", "2023-11-02 12:15", 30, "Chicago"),
//      ("F005", "United", "2023-11-03 08:45", "2023-11-03 11:00", 50, "New York")
//    )
//    val df50 = jideBritish.createDataFrame(Airline_Flight_Delay2)toDF("flight_id", "airline","departure_time","arrival_time", "delay", "destination")
//    val df51 = df50.withColumn("departure date", split($"departure_time", " ").getItem(0))              //splitting date and time
//    val df52 = df51.withColumn("departure time", split($"departure_time", " ").getItem(1)).drop("departure_time")
//    val df53 = df52.withColumn("arrival date", split($"arrival_time", " ").getItem(0))
//    val df54 = df53.withColumn("arrival time", split($"arrival_time", " ").getItem(1)).drop("arrival_time")
//    val df55 = df54.withColumn("departure date", to_date($"departure date"))
//    val df56 = df55.withColumn("departure time", date_format($"departure time".cast("timestamp"), "HH:mm:ss"))
//    val df57 = df56.withColumn("arrival date", to_date($"arrival date"))
//    val df58 = df57.withColumn("arrival time", date_format($"arrival time". cast("timestamp"), "HH:mm:ss"))
//    val df59 = df58.withColumn("delay",  ($"delay")+2)
//    df59.show()
//    val df52 = df51.withColumn("delay_minutes",  datediff(($"departure time"),($"arrival time")))
//    df52.show()
//


    //11.





  }

}
