from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first())
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy(F.col("unit_price_per_unit_price_whole").desc())
w4 = Window.orderBy(F.col("unit_price").asc_nulls_first())
w_part = Window.partitionBy("product_id").orderBy(F.col("quantity_cume_dist").asc_nulls_last())

cleaned = (
    df_order_items
    .filter(F.col("quantity") <= 8)
    .withColumn("unit_price_whole", F.ceil("unit_price"))
)

valid = (
    cleaned
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        "unit_price_whole",
        (F.col("unit_price_whole") * F.col("unit_price")).alias("unit_price_whole_times_unit_price"),
        (F.col("unit_price") / F.nullif("unit_price_whole", F.lit(0))).alias("unit_price_per_unit_price_whole"),
    )
)

kept_keys = (
    valid
    .select("id", "product_id")
    .intersect(
        df_order_items
        .filter(F.col("quantity") < 7)
        .select("id", "product_id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "*",
        F.cume_dist().over(w).alias("quantity_cume_dist"),
        F.sum("unit_price_per_unit_price_whole").over(w2).alias("product_id_sum_unit_price_per_unit_price_whole"),
        F.rank().over(w3).alias("unit_price_per_unit_price_whole_rank"),
    )
)

kept_keys_2 = (
    with_windows
    .select("product_id", "id")
    .intersectAll(
        df_order_items
        .filter(F.col("unit_price").between(220, 390))
        .select("product_id", "id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("unit_price_cume_dist", F.cume_dist().over(w4))
    .withColumn("product_id_count_unit_price_whole_times_unit_price", F.count("unit_price_whole_times_unit_price").over(w2))
)

metrics = (
    enriched
    .withColumn("unit_price_per_unit_price_whole_rank_share", F.round((F.col("unit_price_per_unit_price_whole_rank") / F.sum("unit_price_per_unit_price_whole_rank").over(w2)), 4))
    .withColumn("quantity_cume_dist_rank", F.rank().over(w_part))
)

grouped = (
    metrics
    .groupBy("product_id")
    .agg(
        F.percentile_approx("unit_price_cume_dist", [0.25, 0.5, 0.75]).alias("unit_price_cume_dist_quartiles"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("quantity_cume_dist", [0.25, 0.5, 0.75]).alias("quantity_cume_dist_quartiles"),
        F.percentile_approx("unit_price_whole", 0.5).alias("unit_price_whole_p50"),
        F.percentile_approx("product_id_sum_unit_price_per_unit_price_whole", [0.25, 0.5, 0.75]).alias("product_id_sum_unit_price_per_unit_price_whole_quartiles"),
        F.avg("quantity_cume_dist").alias("avg_quantity_cume_dist"),
    )
    .filter(F.col("avg_quantity_cume_dist") >= 0)
)

result = (
    grouped
    .select(
        "product_id",
        "unit_price_cume_dist_quartiles",
        "quantity_bit_or",
        "quantity_cume_dist_quartiles",
        "unit_price_whole_p50",
        "avg_quantity_cume_dist",
    )
    .orderBy(F.col("avg_quantity_cume_dist").asc_nulls_last(), "quantity_bit_or")
)
