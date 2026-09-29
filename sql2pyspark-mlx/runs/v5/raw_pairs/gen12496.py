from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id")
w2 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id")
w5 = Window.partitionBy("order_id").orderBy(F.col("cumulative_unit_price").desc_nulls_last())
w6 = Window.orderBy(F.col("running_unit_price").desc_nulls_first())

ranked = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_windows = (
    ranked
    .select(
        "id",
        "order_id",
        "unit_price",
        F.sum("unit_price").over(w2).alias("running_unit_price"),
        F.sum("unit_price").over(w_recent).alias("cumulative_unit_price"),
    )
)

matched_keys = (
    with_windows
    .select("id")
    .intersect(
        df_order_items
        .filter(F.col("unit_price") <= 160)
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .filter(F.col("cumulative_unit_price") < 892)
    .select(
        "id",
        "unit_price",
        "running_unit_price",
        "cumulative_unit_price",
        F.round((F.col("cumulative_unit_price") / F.sum("cumulative_unit_price").over(w4)), 4).alias("cumulative_unit_price_share"),
        F.percent_rank().over(w5).alias("cumulative_unit_price_percent_rank"),
        F.cume_dist().over(w6).alias("running_unit_price_cume_dist"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("unit_price") >= 320, "HIGH").otherwise("LOW").alias("unit_price_tier"),
        F.when(F.col("cumulative_unit_price_percent_rank") >= 0.65, "LARGE").otherwise("SMALL").alias("cumulative_unit_price_percent_rank_tier"),
    )
    .agg(
        F.percentile_approx("cumulative_unit_price_share", 0.95).alias("cumulative_unit_price_share_p95"),
        F.count("*").alias("cnt"),
        F.percentile_approx("cumulative_unit_price_share", [0.25, 0.5, 0.75]).alias("cumulative_unit_price_share_quartiles"),
    )
)
