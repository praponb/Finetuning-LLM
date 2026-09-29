from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("start_date_month_end").asc_nulls_first())
w2 = Window.partitionBy("status").orderBy(F.col("start_date").desc_nulls_first(), "id")

cleaned = (
    df_subscriptions
    .filter(F.col("end_date") >= F.to_date(F.lit("2023-04-27")))
    .withColumn("start_date_month_end", F.last_day("start_date"))
    .withColumn("end_date_day", F.dayofmonth("end_date"))
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.credit_limit"))
)

deduped = (
    with_dims
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped_subscriptions = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

base = (
    deduped_subscriptions
    .filter(F.col("start_date") >= F.lit("2023-07-19").cast("date"))
    .withColumn("status_group", F.when(F.col("status").isin(["PAUSED", "ACTIVE"]), "Group A").otherwise("Other"))
)

result = (
    base
    .alias("s")
    .join(df_customers.alias("dim"), F.col("s.customer_id") == F.col("dim.id"), "inner")
    .groupBy(F.col("dim.city").alias("city"), F.col("s.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("s.end_date_day")), 2).alias("total_end_date_day"),
        F.round(F.sum(F.col("s.monthly_fee")), 2).alias("total_monthly_fee"),
    )
    .orderBy(F.col("status").asc_nulls_first())
)
