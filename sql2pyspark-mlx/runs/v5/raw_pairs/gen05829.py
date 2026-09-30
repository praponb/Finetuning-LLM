from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at_period").desc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("status")
w4 = Window.orderBy("amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("amount_bucket").desc_nulls_last())
w6 = Window.orderBy("amount_share", "id")
w7 = Window.orderBy("paid_at", "id")

filtered = (
    df_payments
    .filter(F.col("amount").between(490, 1260) & F.col("method").isNotNull())
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        F.concat("method", F.lit("/"), "status").alias("method_status"),
        F.date_trunc("YEAR", "paid_at").alias("paid_at_period"),
    )
)

deduped = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    deduped
    .select(
        "id",
        "amount",
        "paid_at",
        "method_status",
        "paid_at_period",
        F.ntile(5).over(w2).alias("amount_bucket"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
    )
)

with_windows = (
    enriched
    .filter(F.col("paid_at_period") >= "2023-11-01")
    .select(
        "id",
        "paid_at",
        "method_status",
        "amount_bucket",
        "amount_share",
        F.avg("amount").over(w4).alias("running_avg_amount"),
        F.max("amount").over(w4).alias("running_max_amount"),
        F.count("amount").over(w4).alias("running_count_amount"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("amount_share") >= 0.57)
    .select(
        "*",
        (F.col("running_avg_amount") * F.col("amount_bucket")).alias("running_avg_amount_times_amount_bucket"),
        F.coalesce("running_count_amount", F.lit(0)).alias("running_count_amount_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "amount_bucket",
        "amount_share",
        "running_avg_amount_times_amount_bucket",
        F.rank().over(w5).alias("amount_bucket_rank"),
        F.ntile(5).over(w6).alias("amount_share_bucket"),
        F.lead("running_count_amount_filled", 1).over(w7).alias("next_running_count_amount_filled"),
    )
)
