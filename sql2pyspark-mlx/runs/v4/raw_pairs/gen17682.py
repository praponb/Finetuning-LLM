from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-3, -1)
w_hist = Window.orderBy(F.col("quantity").asc_nulls_last())
w3 = Window.orderBy(F.col("unit_price").desc(), "id")
w4 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_order_items
    .select(
        "*",
        F.avg("quantity").over(w).alias("quantity_avg_3_prior"),
        F.percent_rank().over(w_hist).alias("quantity_percent_rank"),
        F.row_number().over(w3).alias("event_seq"),
        F.sum("unit_price").over(w4).alias("running_unit_price"),
    )
)

result = (
    with_windows
    .rollup("product_id", "order_id")
    .agg(
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.percentile_approx("quantity_percent_rank", [0.25, 0.5, 0.75]).alias("quantity_percent_rank_quartiles"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
    )
    .orderBy(F.col("distinct_order_ids").desc_nulls_last())
)
