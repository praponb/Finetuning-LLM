from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("unit_price")
w2 = Window.orderBy("unit_price", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("order_id").orderBy("quantity", "id")
w5 = Window.partitionBy("order_id")
w6 = Window.partitionBy("product_id").orderBy("order_id_min_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("product_id").orderBy("unit_price_avg_3_prior")
w8 = Window.partitionBy("product_id").orderBy(F.col("quantity_share").asc_nulls_first(), "id")

ranked = (
    df_order_items
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

metrics = (
    ranked
    .select(
        "*",
        F.avg("unit_price").over(w2).alias("unit_price_avg_3_prior"),
        F.round((F.col("quantity") / F.sum("quantity").over(w3)), 4).alias("quantity_share"),
        F.row_number().over(w4).alias("event_seq"),
        F.min("quantity").over(w5).alias("order_id_min_quantity"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "product_id",
        "unit_price_avg_3_prior",
        "quantity_share",
        "event_seq",
        "order_id_min_quantity",
        F.min("quantity_share").over(w6).alias("running_min_quantity_share"),
        F.avg("quantity_share").over(w6).alias("running_avg_quantity_share"),
        F.count("quantity_share").over(w6).alias("running_count_quantity_share"),
        F.cume_dist().over(w7).alias("unit_price_avg_3_prior_cume_dist"),
    )
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w8))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    deduped
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.cost"))
)

result = (
    with_dims
    .groupBy("product_id")
    .agg(
        F.percentile_approx("running_min_quantity_share", 0.9).alias("running_min_quantity_share_p90"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("running_min_quantity_share_p90") > 0)
    .orderBy(F.col("running_min_quantity_share_p90").asc_nulls_last())
)
