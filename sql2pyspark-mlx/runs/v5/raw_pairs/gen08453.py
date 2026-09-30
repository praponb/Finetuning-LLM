from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter((F.col("end_date") > F.to_date(F.lit("2023-11-03"))) & (F.col("status") == "PAUSED"))
    .withColumn("start_date_plus_41d", F.date_add("start_date", 41))
)

result = (
    valid
    .groupBy("customer_id")
    .pivot("status", ["EXPIRED", "ACTIVE"])
    .agg(F.sum("monthly_fee").alias("total"), F.count(F.lit(1)).alias("n"))
)
