from pyspark.sql import functions as F

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("line_total", (F.col("quantity") - F.col("unit_price")))
)

enriched = (
    cleaned
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.discount"), F.col("b.channel"))
)

filtered = (
    enriched
    .filter(F.col("channel").isNotNull() & (F.col("discount") >= 0.18))
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        "line_total",
        "discount",
        "channel",
        F.trim("channel").alias("clean_channel"),
    )
)

base = (
    filtered
    .filter(F.col("line_total").isNotNull())
    .select(
        "id",
        "unit_price",
        "line_total",
        "discount",
        "channel",
        "clean_channel",
        F.when(F.col("channel") == "store", 1).otherwise(0).alias("is_store"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("discount") >= 0.32, "HIGH").otherwise("LOW").alias("discount_tier"),
    )
    .agg(
        F.count_if(F.col("channel") == "web").alias("web_events"),
        F.sum(F.when(F.col("channel") == "store", F.col("discount")).otherwise(0)).alias("store_discount"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.percentile_approx("line_total", [0.25, 0.5, 0.75]).alias("line_total_quartiles"),
    )
    .orderBy(F.col("mobile_events").asc_nulls_first())
)
