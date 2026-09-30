from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        F.when(F.col("cost") >= 90, "High").when(F.col("cost") >= 47, "Medium").otherwise("Low").alias("cost_level"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_filled"),
    )
)

enriched = (
    cleaned
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.shipping_fee"), F.col("p.status").alias("order_status"))
)

enriched_shipments = (
    enriched
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.channel"))
)

base = (
    enriched_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-04-30").cast("date")) & F.col("carrier").isNotNull())
    .withColumn("shipped_date_period", F.date_trunc("year", "shipped_date"))
    .withColumn("shipping_fee_times_cost", (F.col("shipping_fee") * F.col("cost")))
)

grouped = (
    base
    .groupBy(
        F.when(F.col("shipping_fee_times_cost") >= 71, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_times_cost_tier"),
    )
    .agg(
        F.avg("shipping_fee").alias("avg_shipping_fee"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
        ((F.unix_timestamp(F.max("shipped_date_period")) - F.unix_timestamp(F.min("shipped_date_period"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("num_channels") >= 1)
)

result = (
    grouped
    .select(
        "shipping_fee_times_cost_tier",
        "avg_shipping_fee",
        "cost_p95",
        "num_channels",
        "span_minutes",
    )
    .orderBy(F.col("span_minutes").desc_nulls_first())
)
