from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("days_since_order_date", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("channel").orderBy("days_since_order_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc(), "id")

eligible = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id").isin([7, 29, 30])), "left_semi")
)

valid = (
    eligible
    .filter((F.col("order_date") > F.lit("2023-08-07").cast("date")) & F.col("customer_id").isNotNull() & F.col("shipping_fee").isNull())
    .select(
        "id",
        "store_id",
        "channel",
        "shipping_fee",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
        F.round((F.col("shipping_fee") * 1.07), 2).alias("shipping_fee_scaled"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

metrics = (
    valid
    .withColumn("shipping_fee_avg_5_prior", F.avg("shipping_fee").over(w))
    .withColumn("shipping_fee_avg_6", F.avg("shipping_fee").over(w2))
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

remaining_keys = (
    latest
    .select("store_id", "channel")
    .subtract(
        df_orders
        .filter(F.col("customer_id").isNotNull())
        .select("store_id", "channel")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("store_id").isin([18, 28, 38]))
    .select(
        "shipping_fee",
        "shipping_fee_scaled",
        "resolved_channel",
        "shipping_fee_avg_5_prior",
        "shipping_fee_avg_6",
        "latest_rank",
        F.round((F.col("days_since_order_date") * 100), 2).alias("adjusted_days_since_order_date"),
    )
)

stats = (
    base
    .withColumn("shipping_fee_avg_6_tier", F.when(F.col("shipping_fee_avg_6") >= 38, "LARGE").otherwise("SMALL"))
    .withColumn("adjusted_days_since_order_date_tier", F.when(F.col("adjusted_days_since_order_date") >= 84, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["shipping_fee_avg_6_tier", "adjusted_days_since_order_date_tier"], ["shipping_fee_avg_6_tier"]],
        "shipping_fee_avg_6_tier",
        "adjusted_days_since_order_date_tier",
    )
    .agg(
        F.percentile_approx("adjusted_days_since_order_date", [0.25, 0.5, 0.75]).alias("adjusted_days_since_order_date_quartiles"),
        F.percentile_approx("shipping_fee_avg_6", 0.95).alias("shipping_fee_avg_6_p95"),
        F.round(F.avg("shipping_fee_scaled"), 2).alias("avg_shipping_fee_scaled"),
        F.max("adjusted_days_since_order_date").alias("max_adjusted_days_since_order_date"),
    )
)

result = (
    stats
    .select(
        "shipping_fee_avg_6_tier",
        "adjusted_days_since_order_date_tier",
        "shipping_fee_avg_6_p95",
        "avg_shipping_fee_scaled",
    )
    .orderBy(F.col("shipping_fee_avg_6_tier").desc())
)
