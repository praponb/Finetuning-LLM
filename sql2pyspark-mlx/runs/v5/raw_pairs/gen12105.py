from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("end_date") >= F.lit("2023-08-11").cast("date"))
    .withColumn("start_date_plus_months", F.add_months("start_date", 12))
)

result = (
    base
    .groupBy("customer_id")
    .pivot("plan", ["enterprise", "basic"])
    .agg(F.sum("monthly_fee").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
