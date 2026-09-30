from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("monthly_fee", "id")

deduped = (
    df_subscriptions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .groupBy("status")
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.percentile_approx("monthly_fee", 0.5).alias("monthly_fee_p50"),
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee"))).alias("expired_total_monthly_fee"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .filter(F.col("expired_total_monthly_fee") > 0)
    .orderBy(F.col("avg_monthly_fee").asc_nulls_last())
)
