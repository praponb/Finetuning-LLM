from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").asc_nulls_first(), "id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("line_total", "id")
w5 = Window.partitionBy("order_id", "product_id")
w6 = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc_nulls_last())
w7 = Window.partitionBy("order_id").orderBy("quantity_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("quantity_share").desc_nulls_last())
w_part = Window.orderBy("unit_price", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("order_id").isin([10, 16]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    deduped
    .filter(F.col("unit_price") < 370)
    .select(
        "id",
        "order_id",
        "unit_price",
        F.round((F.col("quantity") / F.sum("quantity").over(w2)), 4).alias("quantity_share"),
        F.sum("unit_price").over(w3).alias("running_unit_price"),
        F.lead("line_total", 2).over(w4).alias("next_line_total"),
        F.corr("unit_price", "quantity").over(w5).alias("corr_unit_price_quantity"),
        F.rank().over(w6).alias("quantity_rank"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "unit_price",
        "quantity_share",
        "running_unit_price",
        "next_line_total",
        "corr_unit_price_quantity",
        "quantity_rank",
        F.sum("unit_price").over(w7).alias("running_sum_unit_price"),
        F.min("unit_price").over(w7).alias("running_min_unit_price"),
        F.count("unit_price").over(w7).alias("running_count_unit_price"),
        F.percent_rank().over(w8).alias("quantity_share_percent_rank"),
    )
)

base_order_items = (
    with_windows
    .filter(F.col("quantity_rank") < 7)
    .select(
        "*",
        (F.col("corr_unit_price_quantity") - F.col("quantity_share_percent_rank")).alias("corr_unit_price_quantity_minus_quantity_share_percent_rank"),
    )
)

enriched_order_items = (
    base_order_items
    .withColumn("last4_sum_running_count_unit_price", F.sum("running_count_unit_price").over(w_part))
    .withColumn("last4_max_running_count_unit_price", F.max("running_count_unit_price").over(w_part))
    .withColumn("last4_avg_running_count_unit_price", F.avg("running_count_unit_price").over(w_part))
)

result = (
    enriched_order_items
    .withColumn("quantity_rank_tier", F.when(F.col("quantity_rank") >= 9, "LARGE").otherwise("SMALL"))
    .rollup("quantity_rank_tier")
    .agg(
        F.percentile_approx("last4_max_running_count_unit_price", [0.25, 0.5, 0.75]).alias("last4_max_running_count_unit_price_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.countDistinct("id").alias("distinct_ids_3"),
        F.round(F.avg("corr_unit_price_quantity"), 2).alias("avg_corr_unit_price_quantity"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("avg_corr_unit_price_quantity") > 0)
    .orderBy(F.col("distinct_ids_3").desc_nulls_first())
)
