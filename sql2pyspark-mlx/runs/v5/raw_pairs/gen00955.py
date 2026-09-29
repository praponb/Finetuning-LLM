from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") < 430)
    .orderBy("order_month", "status")
    .limit(1)
)
