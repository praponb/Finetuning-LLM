from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id")
w3 = Window.partitionBy("product_id").orderBy("dedup_rank", "id")
w_hist = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").asc_nulls_last())
w5 = Window.partitionBy("order_id").orderBy("order_date", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("order_id").orderBy("next_quantity", "id")
w7 = Window.partitionBy("product_id", "order_id").orderBy("quantity_dense_rank", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.partitionBy("order_id").orderBy(F.col("quantity_dense_rank_avg_5").desc())
w9 = Window.orderBy(F.col("unit_price_avg_2_prior").desc_nulls_first())
w10 = Window.orderBy("next_quantity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_order_items
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.product_id",
        "a.quantity",
        "a.unit_price",
        F.col("d.order_date"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    ranked
    .select(
        "*",
        F.lead("unit_price", 1).over(w2).alias("next_unit_price"),
        F.lead("quantity", 2, 0).over(w3).alias("next_quantity"),
        F.dense_rank().over(w_hist).alias("quantity_dense_rank"),
    )
)

with_windows = (
    metrics
    .withColumn("unit_price_avg_2_prior", F.avg("unit_price").over(w5))
    .withColumn("next_quantity_bucket", F.ntile(5).over(w6))
    .withColumn("quantity_dense_rank_avg_5", F.avg("quantity_dense_rank").over(w7))
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.rank().over(w8))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "next_unit_price",
        "quantity_dense_rank",
        "unit_price_avg_2_prior",
        "next_quantity_bucket",
    )
)

with_windows_order_items = (
    latest
    .select(
        "*",
        F.percent_rank().over(w9).alias("unit_price_avg_2_prior_percent_rank"),
        F.sum("quantity_dense_rank").over(w10).alias("cumulative_quantity_dense_rank"),
    )
)

cleaned = (
    with_windows_order_items
    .filter(F.col("cumulative_quantity_dense_rank") <= 28)
    .select(
        "*",
        F.round((F.col("unit_price_avg_2_prior_percent_rank") / F.col("unit_price_avg_2_prior")), 2).alias("unit_price_avg_2_prior_percent_rank_per_unit_price_avg_2_prior"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("unit_price_avg_2_prior_percent_rank_per_unit_price_avg_2_prior") >= 90, "LARGE").otherwise("SMALL").alias("unit_price_avg_2_prior_percent_rank_per_unit_price_avg_2_prior_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("unit_price_avg_2_prior", [0.25, 0.5, 0.75]).alias("unit_price_avg_2_prior_quartiles"),
        F.round(F.avg("unit_price_avg_2_prior_percent_rank"), 2).alias("avg_unit_price_avg_2_prior_percent_rank"),
        F.countDistinct("id").alias("distinct_ids_3"),
        F.sum("next_unit_price").alias("total_next_unit_price"),
    )
    .orderBy("unit_price_avg_2_prior_percent_rank_per_unit_price_avg_2_prior_tier")
)
