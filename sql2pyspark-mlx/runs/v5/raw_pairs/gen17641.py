from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("line_total").asc_nulls_first())

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)

valid = (
    filtered
    .filter(F.col("unit_price") >= 380)
    .select(
        "id",
        "order_id",
        "quantity",
        "line_total",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
    )
)

valid_order_items = (
    valid
    .filter(F.col("least_unit_price_quantity") < 32)
    .withColumn("least_unit_price_quantity_plus_line_total", (F.col("least_unit_price_quantity") + F.col("line_total")))
)

deduped = (
    valid_order_items
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

matched_keys = (
    deduped
    .select("id", "order_id")
    .intersectAll(
        df_order_items
        .filter(F.col("quantity").isNotNull())
        .select("id", "order_id")
    )
)

verified = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

filtered_order_items = (
    verified
    .filter(F.col("order_id").isin([14, 15, 24]))
    .select(
        "id",
        "least_unit_price_quantity",
        "least_unit_price_quantity_plus_line_total",
        "dedup_rank",
        F.round((F.col("least_unit_price_quantity_plus_line_total") * 0.9), 2).alias("least_unit_price_quantity_plus_line_total_scaled"),
    )
)

result = (
    filtered_order_items
    .groupBy(
        F.when(F.col("least_unit_price_quantity_plus_line_total_scaled") >= 77, "LARGE").otherwise("SMALL").alias("least_unit_price_quantity_plus_line_total_scaled_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("least_unit_price_quantity_plus_line_total"), 2).alias("avg_least_unit_price_quantity_plus_line_total"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("least_unit_price_quantity_plus_line_total", [0.25, 0.5, 0.75]).alias("least_unit_price_quantity_plus_line_total_quartiles"),
        F.round(F.avg("least_unit_price_quantity_plus_line_total"), 2).alias("avg_least_unit_price_quantity_plus_line_total_2"),
    )
    .orderBy(F.col("avg_least_unit_price_quantity_plus_line_total_2").desc())
)
