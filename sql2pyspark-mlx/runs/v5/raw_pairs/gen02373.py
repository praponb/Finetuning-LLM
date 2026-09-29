from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")

with_windows = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.avg("quantity").over(w_hist).alias("running_avg_quantity"),
        F.count("quantity").over(w_hist).alias("running_count_quantity"),
        F.min("quantity").over(w_hist).alias("running_min_quantity"),
        F.ntile(4).over(w2).alias("unit_price_bucket"),
    )
)

result = (
    with_windows
    .groupBy(
        "product_id",
        F.when(F.col("running_count_quantity") >= 13, "PREMIUM").otherwise("STANDARD").alias("running_count_quantity_tier"),
    )
    .agg(
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.round(F.avg("running_avg_quantity"), 2).alias("avg_running_avg_quantity"),
        F.percentile_approx("running_min_quantity", [0.25, 0.5, 0.75]).alias("running_min_quantity_quartiles"),
    )
    .orderBy("product_id")
)
