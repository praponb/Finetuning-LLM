from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("unit_price").desc_nulls_last())
w3 = Window.partitionBy("product_id").orderBy(F.col("running_sum_stock_qty").desc_nulls_last(), "id")
w4 = Window.partitionBy("order_id", "product_id").orderBy("running_avg_stock_qty", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy(F.col("stock_qty").asc_nulls_last())
w6 = Window.orderBy(F.col("last4_count_running_avg_stock_qty").desc(), "id")
w7 = Window.orderBy(F.col("stock_qty_dense_rank").desc_nulls_last())
w8 = Window.orderBy("running_avg_stock_qty", "id")
w9 = Window.orderBy("running_sum_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_order_items
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.brand").isin(["Globex", "Initech", "Stark"]))
    .select("s.*", F.col("b.stock_qty"), F.col("b.launch_date"))
)

with_windows = (
    enriched
    .select(
        "id",
        "order_id",
        "product_id",
        "stock_qty",
        "launch_date",
        F.sum("stock_qty").over(w).alias("running_sum_stock_qty"),
        F.avg("stock_qty").over(w).alias("running_avg_stock_qty"),
        F.min("stock_qty").over(w).alias("running_min_stock_qty"),
        F.cume_dist().over(w2).alias("unit_price_cume_dist"),
    )
)

deduped = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

metrics = (
    deduped
    .select(
        "id",
        "running_sum_stock_qty",
        "running_avg_stock_qty",
        "running_min_stock_qty",
        F.min("running_avg_stock_qty").over(w4).alias("last4_min_running_avg_stock_qty"),
        F.sum("running_avg_stock_qty").over(w4).alias("last4_sum_running_avg_stock_qty"),
        F.count("running_avg_stock_qty").over(w4).alias("last4_count_running_avg_stock_qty"),
        F.dense_rank().over(w5).alias("stock_qty_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "running_sum_stock_qty",
        "running_avg_stock_qty",
        "running_min_stock_qty",
        "last4_sum_running_avg_stock_qty",
        "last4_count_running_avg_stock_qty",
        F.row_number().over(w6).alias("row_seq"),
        F.cume_dist().over(w7).alias("stock_qty_dense_rank_cume_dist"),
        F.ntile(3).over(w8).alias("running_avg_stock_qty_bucket"),
        F.sum("running_min_stock_qty").over(w9).alias("cumulative_running_min_stock_qty"),
    )
)
