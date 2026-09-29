from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("quantity").between(6, 7))
    .withColumn("line_total", (F.col("quantity") + F.col("unit_price")))
)

enriched = (
    base
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.quantity",
        "src.unit_price",
        F.col("o.amount"),
    )
)

remaining_keys = (
    enriched
    .select("order_id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") < 5)
        .select("order_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.amount").between(1110, 1550))
    .select("e.order_id", "e.unit_price", F.col("o.status"), F.col("o.discount"))
)

summary = (
    joined
    .groupBy(
        F.when(F.col("discount") >= 0.37, "HIGH").otherwise("LOW").alias("discount_tier"),
        "status",
    )
    .agg(
        F.min("discount").alias("min_discount"),
        F.sum("discount").alias("total_discount"),
        F.countDistinct("status").alias("distinct_status"),
        F.percentile_approx("discount", 0.5).alias("discount_p50"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    summary
    .select(
        "discount_tier",
        "status",
        "min_discount",
        "total_discount",
        "distinct_status",
        "discount_p50",
        "status_list",
    )
    .orderBy(F.col("distinct_status").asc_nulls_first())
)
