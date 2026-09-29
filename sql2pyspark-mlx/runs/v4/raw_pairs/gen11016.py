from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .withColumn("status_prefix", F.substring("status", 1, 4))
)

start_month_summary = (
    src
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"), "status")
    .agg(F.min("end_date").alias("first_end_date"))
)

result = start_month_summary.orderBy("status", F.col("first_end_date").desc())
