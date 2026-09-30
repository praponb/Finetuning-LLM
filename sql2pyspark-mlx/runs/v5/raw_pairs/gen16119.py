from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("avg_monthly_fee_2").desc())

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .withColumn("monthly_fee_band", F.when(F.col("monthly_fee") >= 182, "High").when(F.col("monthly_fee") >= 90, "Medium").otherwise("Low"))
)

result = (
    cleaned
    .groupBy("status", F.date_trunc("week", "start_date").alias("week_start"))
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee_2"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .withColumn("avg_monthly_fee_2_dense_rank", F.dense_rank().over(w))
    .orderBy("status")
)
