from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", F.date_trunc("MONTH", "end_date").alias("end_month"))
    .agg(
        F.min("start_date").alias("first_start_date"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_count"),
    )
    .filter(F.col("standard_count") >= 9)
    .select("status", "end_month", "first_start_date", "standard_count")
)
