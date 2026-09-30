from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")
w2 = Window.partitionBy("status", "order_id")
w3 = Window.partitionBy("carrier").orderBy(F.col("shipping_fee").desc())
w4 = Window.partitionBy("week_start").orderBy(F.col("min_shipping_fee_percent_rank_plus_status_max_shipping_fee").desc())

cleaned = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        F.when(F.col("cost") >= 89, "High").when(F.col("cost") >= 19, "Medium").otherwise("Low").alias("cost_band"),
        F.when(F.col("status").isin(["RETURNED", "LOST"]), "Priority").otherwise("Other").alias("status_group"),
    )
)

matched_keys = (
    cleaned
    .select("id", "carrier")
    .subtract(
        df_shipments
        .filter(F.col("shipped_date").between("2023-09-01", "2025-07-01"))
        .select("id", "carrier")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.status").isin(["COMPLETED", "PENDING", "CANCELLED"]))
    .select(
        "s.id",
        "s.order_id",
        "s.carrier",
        "s.delivered_date",
        "s.cost_band",
        F.col("d.shipping_fee"),
        F.col("d.order_date"),
        F.col("d.status"),
    )
)

metrics = (
    enriched
    .select(
        "delivered_date",
        "cost_band",
        "shipping_fee",
        "order_date",
        F.count("shipping_fee").over(w).alias("carrier_count_shipping_fee"),
        F.max("shipping_fee").over(w2).alias("status_max_shipping_fee"),
        F.percent_rank().over(w3).alias("shipping_fee_percent_rank"),
    )
)

filtered = (
    metrics
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-06-18"))) & F.col("carrier_count_shipping_fee").between(12, 15))
    .select(
        "*",
        (F.col("shipping_fee_percent_rank") + F.col("status_max_shipping_fee")).alias("shipping_fee_percent_rank_plus_status_max_shipping_fee"),
    )
)

stats = (
    filtered
    .groupBy(
        F.date_trunc("WEEK", "order_date").alias("week_start"),
        F.date_trunc("MONTH", "delivered_date").alias("activity_month"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("shipping_fee_percent_rank"), 2).alias("avg_shipping_fee_percent_rank"),
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.min("shipping_fee_percent_rank_plus_status_max_shipping_fee").alias("min_shipping_fee_percent_rank_plus_status_max_shipping_fee"),
    )
    .withColumn("min_shipping_fee_percent_rank_plus_status_max_shipping_fee_dense_rank", F.dense_rank().over(w4))
)

result = (
    stats
    .select(
        "week_start",
        "activity_month",
        "avg_shipping_fee_percent_rank",
        "max_shipping_fee",
        "min_shipping_fee_percent_rank_plus_status_max_shipping_fee_dense_rank",
    )
    .orderBy(F.col("max_shipping_fee").desc_nulls_first())
)
