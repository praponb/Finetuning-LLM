from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant")
w2 = Window.orderBy(F.col("amount").asc_nulls_last())
w3 = Window.partitionBy("merchant", "txn_type").orderBy("amount", "id")
w4 = Window.partitionBy("amount_share_tier").orderBy(F.col("amount_bucket_p50").desc())

enriched = (
    df_transactions
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w)), 4))
    .withColumn("amount_percent_rank", F.percent_rank().over(w2))
    .withColumn("amount_bucket", F.ntile(10).over(w3))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("amount_share") >= 0.9, "LARGE").otherwise("SMALL").alias("amount_share_tier"),
        "merchant",
        F.date_trunc("MONTH", "txn_time").alias("activity_month"),
    )
    .agg(
        F.max("txn_time").alias("ended_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("amount_bucket", 0.5).alias("amount_bucket_p50"),
    )
    .withColumn("amount_bucket_p50_percent_rank", F.percent_rank().over(w4))
    .orderBy("amount_share_tier")
)
