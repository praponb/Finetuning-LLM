from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("product_id").orderBy("quantity", "id")
w3 = Window.partitionBy("order_id").orderBy("quantity_rounded", "id").rowsBetween(-5, Window.currentRow)
w_part_2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("unit_price", "id")
w6 = Window.partitionBy("product_id")
w_recent = Window.partitionBy("order_id")
w8 = Window.partitionBy("order_id").orderBy(F.col("running_unit_price_filled").desc_nulls_last())
w9 = Window.partitionBy("product_id", "order_id").orderBy("prev_unit_price_filled")
w10 = Window.orderBy(F.col("quantity").desc_nulls_last())

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("unit_price_filled", F.coalesce("unit_price", F.lit(0)))
    .withColumn("quantity_rounded", F.round("quantity", 0))
)

with_windows = (
    valid
    .withColumn("first_known_quantity_rounded", F.first("quantity_rounded", ignorenulls=True).over(w_part))
    .withColumn("prev_unit_price_filled", F.lag("unit_price_filled", 1).over(w_grp))
    .withColumn("quantity_avg_5", F.avg("quantity").over(w3))
    .withColumn("running_unit_price_filled", F.sum("unit_price_filled").over(w_part_2))
    .withColumn("unit_price_bucket", F.ntile(4).over(w5))
)

matched_keys = (
    with_windows
    .select("id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("product_id").isNotNull())
        .select("id", "product_id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "*",
        F.round((F.col("quantity") / F.sum("quantity").over(w6)), 4).alias("quantity_share"),
        F.stddev_samp("running_unit_price_filled").over(w_recent).alias("order_id_stddev_samp_running_unit_price_filled"),
        F.rank().over(w8).alias("running_unit_price_filled_rank"),
        F.percent_rank().over(w9).alias("prev_unit_price_filled_percent_rank"),
    )
)

metrics_order_items = (
    metrics
    .withColumn("quantity_rank", F.rank().over(w10))
    .withColumn("unit_price_bucket_share", F.round((F.col("unit_price_bucket") / F.sum("unit_price_bucket").over(w6)), 4))
)

daily = (
    metrics_order_items
    .groupBy(
        F.when(F.col("running_unit_price_filled") >= 404, "PREMIUM").otherwise("STANDARD").alias("running_unit_price_filled_tier"),
        F.when(F.col("running_unit_price_filled_rank") >= 7, "LARGE").otherwise("SMALL").alias("running_unit_price_filled_rank_tier"),
        F.when(F.col("quantity") >= 8, "HIGH").otherwise("LOW").alias("quantity_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg("order_id_stddev_samp_running_unit_price_filled").alias("avg_order_id_stddev_samp_running_unit_price_filled"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
)

result = (
    daily
    .select(
        "running_unit_price_filled_tier",
        "running_unit_price_filled_rank_tier",
        "quantity_tier",
        "cnt",
        "avg_order_id_stddev_samp_running_unit_price_filled",
        "distinct_order_ids",
    )
    .orderBy(
        F.col("avg_order_id_stddev_samp_running_unit_price_filled").asc_nulls_first(),
    )
)
