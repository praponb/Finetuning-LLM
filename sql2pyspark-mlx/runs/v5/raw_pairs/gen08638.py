from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["Sales", "Marketing"])
    .agg(F.sum("budget").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "location",
        F.col("Sales_amount").alias("sales_amount"),
        F.col("Sales_cnt").alias("sales_cnt"),
        F.col("Marketing_amount").alias("marketing_amount"),
        F.col("Marketing_cnt").alias("marketing_cnt"),
    )
)
