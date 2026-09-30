from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.orderBy("monthly_fee", "id")
w3 = Window.partitionBy("status").orderBy(F.col("monthly_fee_share").desc_nulls_last(), "id")
w4 = Window.partitionBy("customer_id").orderBy("monthly_fee_whole", "id")
w5 = Window.partitionBy("status")
w6 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")
w7 = Window.partitionBy("customer_id", "status").orderBy(F.col("monthly_fee_share").desc_nulls_first())

filtered = (
    df_subscriptions
    .filter(F.col("status") == "CANCELLED")
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
    )
)

scored = (
    filtered
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4))
    .withColumn("monthly_fee_bucket", F.ntile(3).over(w2))
)

ranked = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

metrics = (
    ranked
    .withColumn("monthly_fee_whole_bucket", F.ntile(4).over(w4))
    .withColumn("monthly_fee_bucket_share", F.round((F.col("monthly_fee_bucket") / F.sum("monthly_fee_bucket").over(w5)), 4))
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("monthly_fee_share_rank", F.rank().over(w7))
)

result = (
    metrics
    .cube("status", "customer_id")
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("status").alias("distinct_status"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.countDistinct("status").alias("distinct_status_2"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
    )
    .filter(F.col("num_status") >= 1)
)
