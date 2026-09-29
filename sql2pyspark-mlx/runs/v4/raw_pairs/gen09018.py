from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("launch_date").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("launch_date", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("product_id", "order_id").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("order_id").orderBy("quantity_whole", "id")
w_running = Window.orderBy(F.col("quantity_whole").desc_nulls_first())
w7 = Window.partitionBy("order_id")
w8 = Window.orderBy(F.col("quantity_whole_bucket").desc_nulls_last())
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.avg("cost").alias("avg_shipment_cost"))

base = (
    df_order_items
    .filter(F.col("order_id").isin([7, 31]))
    .withColumn("quantity_whole", F.ceil("quantity"))
)

joined = (
    base
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.launch_date"), F.col("p.product_name"))
)

deduped = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.avg("quantity_whole").over(w2).alias("quantity_whole_avg_3_prior"),
        F.sum("quantity").over(w3).alias("cumulative_quantity"),
        F.avg("unit_price").over(w4).alias("unit_price_avg_3_prior"),
        F.ntile(3).over(w5).alias("quantity_whole_bucket"),
    )
)

remaining_keys = (
    enriched
    .select("product_id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("unit_price") >= 370)
        .select("product_id", "order_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("quantity_whole_cume_dist", F.cume_dist().over(w_running))
    .withColumn("unit_price_avg_3_prior_share", F.round((F.col("unit_price_avg_3_prior") / F.sum("unit_price_avg_3_prior").over(w7)), 4))
    .withColumn("quantity_whole_bucket_percent_rank", F.percent_rank().over(w8))
)

grouped = (
    with_windows
    .groupBy(
        F.when(F.col("quantity_whole") >= 79, "PREMIUM").otherwise("STANDARD").alias("quantity_whole_tier"),
        "order_id",
        F.when(F.col("quantity_whole_bucket_percent_rank") >= 0.67, "LARGE").otherwise("SMALL").alias("quantity_whole_bucket_percent_rank_tier"),
    )
    .agg(
        F.percentile_approx("quantity_whole_cume_dist", [0.25, 0.5, 0.75]).alias("quantity_whole_cume_dist_quartiles"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
    )
)

result = (
    grouped
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "quantity_whole_tier",
        "order_id",
        "quantity_whole_bucket_percent_rank_tier",
        "quantity_bit_and",
        "quantity_quartiles",
        F.col("avg_shipment_cost"),
    )
    .orderBy(F.col("quantity_whole_tier").desc_nulls_last())
)
