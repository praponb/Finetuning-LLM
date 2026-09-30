from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy(F.col("monthly_fee").asc_nulls_first())
w2 = Window.partitionBy("customer_id", "plan").orderBy("monthly_fee")
w3 = Window.partitionBy("customer_id")

filtered = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-05-12").cast("date")) & F.col("customer_id").isNotNull())
    .select("*", F.last_day("start_date").alias("start_date_month_end"))
)

metrics = (
    filtered
    .withColumn("monthly_fee_rank", F.rank().over(w))
    .withColumn("monthly_fee_rank_2", F.rank().over(w2))
    .withColumn("customer_id_max_monthly_fee", F.max("monthly_fee").over(w3))
)

with_dims = (
    metrics
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.name"), F.col("d.city"))
)

pivoted = (
    with_dims
    .groupBy("city")
    .pivot("status", ["CANCELLED", "PAUSED", "EXPIRED"])
    .agg(F.avg("monthly_fee_rank_2").alias("total"), F.count(F.lit(1)).alias("n"))
)

result = (
    pivoted
    .select("city", "CANCELLED_total", "PAUSED_total", "PAUSED_n", "EXPIRED_total")
)
