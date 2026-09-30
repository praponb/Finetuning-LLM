from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy(F.col("start_date").asc_nulls_last(), "id")

ranked = (
    df_subscriptions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("monthly_fee") >= 231, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
        F.date_trunc("week", "start_date").alias("week_start"),
    )
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.avg(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee"))).alias("cancelled_avg_monthly_fee"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee_2"),
    )
    .orderBy(F.col("monthly_fee_tier").desc())
)
