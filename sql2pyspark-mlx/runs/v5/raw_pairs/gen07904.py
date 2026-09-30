from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("amount_filled", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy(F.col("paid_at").asc_nulls_last(), "id")
w5 = Window.partitionBy("channel").orderBy("paid_at", "id")
w6 = Window.partitionBy("channel").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-06-07").cast("date")) & (F.col("method") == "cash"))
    .select(
        "*",
        F.round("amount", 1).alias("amount_rounded"),
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

with_dims = (
    sessions_payments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.channel"), F.col("d.discount"))
)

metrics = (
    with_dims
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "is_new_session",
        "burst_id",
        "channel",
        F.min("discount").over(w3).alias("last7_min_discount"),
        F.avg("discount").over(w3).alias("last7_avg_discount"),
        F.max("discount").over(w3).alias("last7_max_discount"),
    )
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "method",
        "paid_at",
        "burst_id",
        "channel",
        "last7_min_discount",
        "last7_avg_discount",
        "last7_max_discount",
    )
)

flagged = (
    ranked
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w5) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id_2", F.sum("is_new_burst").over(w6))

result = (
    sessionized
    .select(
        "id",
        "method",
        "burst_id",
        "channel",
        "last7_min_discount",
        "last7_avg_discount",
        "last7_max_discount",
        F.when(F.col("channel").rlike("^(store|mobile)$"), "CORE").when(F.col("channel").rlike("^(web|partner)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("channel_zone"),
    )
)
