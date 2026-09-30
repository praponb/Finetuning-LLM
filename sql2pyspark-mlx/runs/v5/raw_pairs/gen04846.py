from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("start_date").asc_nulls_first(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("standard_count").desc())

deduped = (
    df_subscriptions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy("status", F.date_trunc("WEEK", "end_date").alias("week_start"))
    .agg(
        F.max(F.when(F.col("plan") == "enterprise", F.col("monthly_fee"))).alias("enterprise_max_monthly_fee"),
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_count"),
    )
    .withColumn("standard_count_rank", F.rank().over(w2))
    .orderBy(F.col("monthly_fee_stddev").asc_nulls_first())
)
