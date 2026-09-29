from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "order_status").orderBy(F.col("order_date").asc_nulls_first(), "id")
w2 = Window.partitionBy("channel", "order_id").orderBy(F.col("status_length").desc(), "id")

filtered = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (~F.col("method").eqNullSafe(F.lit("paypal"))))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.length("status").alias("status_length"),
        F.date_trunc("year", "paid_at").alias("paid_at_period"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("resolved_method"),
    )
)

cleaned = (
    filtered
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "status",
        "status_length",
        "resolved_method",
        F.ceil("amount").alias("amount_whole"),
        F.col("paid_at").cast("date").alias("paid_at_day"),
    )
)

enriched = (
    cleaned
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.order_date"))
)

enriched_payments = (
    enriched
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.status_length",
        "a.resolved_method",
        "a.order_date",
        F.col("d.status").alias("order_status"),
        F.col("d.order_date").alias("order_order_date"),
        F.col("d.channel"),
    )
)

ranked = (
    enriched_payments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

latest = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "order_id",
        "status_length",
        "resolved_method",
        "order_date",
        "order_order_date",
        "channel",
        "dedup_rank",
    )
)

stats = (
    latest
    .groupBy(
        F.when(F.col("status_length") >= 63, "HIGH").otherwise("LOW").alias("status_length_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max(F.when(F.col("channel") == "web", F.col("status_length"))).alias("web_max_status_length"),
        F.round(F.sum("status_length") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("status_length_per_store"),
        F.count("*").alias("row_count_2"),
    )
)

result = (
    stats
    .select("status_length_tier", "row_count", "status_length_per_store", "row_count_2")
    .orderBy("row_count")
)
