from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc_nulls_first(), "id")
w_hist = Window.partitionBy("product_id", "order_id").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("order_id", "product_id")
w_grp = Window.partitionBy("product_id")
w_running = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")
w7 = Window.orderBy(F.col("corr_quantity_unit_price").desc_nulls_first())
w8 = Window.partitionBy("order_id").orderBy("corr_quantity_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("product_id").orderBy("cumulative_dedup_rank_minus_running_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("order_id", "product_id").orderBy("abs_corr_quantity_unit_price", "id")

ranked = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    ranked
    .withColumn("running_unit_price", F.sum("unit_price").over(w_hist))
    .withColumn("unit_price_avg_5_prior", F.avg("unit_price").over(w3))
    .withColumn("corr_quantity_unit_price", F.corr("quantity", "unit_price").over(w4))
)

filtered = (
    scored
    .filter(F.col("corr_quantity_unit_price") >= -0.3)
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "dedup_rank",
        "running_unit_price",
        "corr_quantity_unit_price",
        (F.col("dedup_rank") - F.col("running_unit_price")).alias("dedup_rank_minus_running_unit_price"),
        F.abs("corr_quantity_unit_price").alias("abs_corr_quantity_unit_price"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.sum("dedup_rank_minus_running_unit_price").over(w_grp).alias("product_id_sum_dedup_rank_minus_running_unit_price"),
        F.ntile(5).over(w_running).alias("unit_price_bucket"),
        F.sum("abs_corr_quantity_unit_price").over(w4).alias("order_id_sum_abs_corr_quantity_unit_price"),
        F.percent_rank().over(w7).alias("corr_quantity_unit_price_percent_rank"),
        F.sum("dedup_rank_minus_running_unit_price").over(w8).alias("cumulative_dedup_rank_minus_running_unit_price"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.sum("abs_corr_quantity_unit_price").over(w9).alias("running_abs_corr_quantity_unit_price"),
        F.lag("abs_corr_quantity_unit_price", 1).over(w10).alias("prev_abs_corr_quantity_unit_price"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("corr_quantity_unit_price") >= 0.77, "LARGE").otherwise("SMALL").alias("corr_quantity_unit_price_tier"),
        "order_id",
    )
    .agg(
        F.max("abs_corr_quantity_unit_price").alias("max_abs_corr_quantity_unit_price"),
        F.percentile_approx("abs_corr_quantity_unit_price", 0.95).alias("abs_corr_quantity_unit_price_p95"),
    )
    .orderBy(F.col("max_abs_corr_quantity_unit_price").desc_nulls_last())
)
