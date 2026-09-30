from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity_str").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("unit_price").asc_nulls_last())
w3 = Window.partitionBy("product_id", "channel").orderBy("order_date", "id").rowsBetween(-3, -1)
w4 = Window.orderBy("greatest_dedup_rank_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id")
w6 = Window.partitionBy("product_id").orderBy(F.col("quantity_share").desc_nulls_first())
w_part = Window.partitionBy("channel")
w_running = Window.partitionBy("product_id").orderBy(F.col("greatest_dedup_rank_unit_price").desc(), "id")
w9 = Window.partitionBy("product_id").orderBy(F.col("discount").asc_nulls_last())
w10 = Window.partitionBy("product_id").orderBy(F.col("quantity_stddev").desc())

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
        F.col("quantity").cast("double").alias("quantity_str"),
    )
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

deduped = (
    ranked
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    deduped
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") < 310))
    .select(
        "*",
        F.greatest("dedup_rank", "unit_price").alias("greatest_dedup_rank_unit_price"),
        F.round("greatest_unit_price_quantity", 2).alias("greatest_unit_price_quantity_rounded"),
    )
)

enriched = (
    cleaned
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .filter(F.col("d.amount") > 1570)
    .select("e.*", F.col("d.channel"), F.col("d.discount"), F.col("d.order_date"))
)

enriched_order_items = (
    enriched
    .withColumn("greatest_dedup_rank_unit_price_avg_3_prior", F.avg("greatest_dedup_rank_unit_price").over(w3))
    .withColumn("running_unit_price", F.sum("unit_price").over(w4))
    .withColumn("quantity_share", F.round((F.col("quantity") / F.sum("quantity").over(w5)), 4))
)

with_windows = (
    enriched_order_items
    .select(
        "*",
        F.dense_rank().over(w6).alias("quantity_share_dense_rank"),
        F.corr("quantity", "unit_price").over(w_part).alias("corr_quantity_unit_price"),
        F.ntile(3).over(w_running).alias("greatest_dedup_rank_unit_price_bucket"),
        F.cume_dist().over(w9).alias("discount_cume_dist"),
    )
)

result = (
    with_windows
    .groupBy("product_id", "channel")
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.percentile_approx("greatest_unit_price_quantity", [0.25, 0.5, 0.75]).alias("greatest_unit_price_quantity_quartiles"),
    )
    .withColumn("quantity_stddev_percent_rank", F.percent_rank().over(w10))
    .orderBy(F.col("channel").asc_nulls_last())
)
