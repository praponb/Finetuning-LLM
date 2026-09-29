from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("amount", "id")
w3 = Window.partitionBy("status")
w4 = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w6 = Window.orderBy("running_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("running_amount", "id").rowsBetween(-2, -1)
w8 = Window.orderBy(F.col("running_amount").desc())

cleaned = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-05-24 04:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.date_format("paid_at", "MMM yyyy").alias("paid_at_label"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
    .select(
        "id",
        "paid_at_label",
        F.sum("amount").over(w).alias("running_amount"),
        F.ntile(3).over(w2).alias("amount_bucket"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
        F.first("method", ignorenulls=True).over(w4).alias("first_known_method"),
        F.ntile(10).over(w5).alias("amount_bucket_2"),
    )
)

valid = (
    with_windows
    .filter(F.col("amount_bucket_2") <= 7)
    .select(
        "*",
        F.round((F.col("amount_share") / F.col("amount_bucket_2")), 2).alias("amount_share_per_amount_bucket_2"),
    )
)

scored = (
    valid
    .withColumn("first_known_amount_bucket", F.first("amount_bucket", ignorenulls=True).over(w6))
    .withColumn("running_amount_avg_2_prior", F.avg("running_amount").over(w7))
    .withColumn("running_amount_percent_rank", F.percent_rank().over(w8))
)

daily = (
    scored
    .groupBy(
        F.when(F.col("amount_bucket") >= 8, "HIGH").otherwise("LOW").alias("amount_bucket_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("running_amount").alias("min_running_amount"),
        F.min("amount_share").alias("min_amount_share"),
        F.percentile_approx("amount_bucket_2", [0.25, 0.5, 0.75]).alias("amount_bucket_2_quartiles"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    daily
    .select(
        "amount_bucket_tier",
        "distinct_ids",
        "min_running_amount",
        "amount_bucket_2_quartiles",
        "distinct_ids_2",
        "num_rows",
    )
    .orderBy("distinct_ids")
)
