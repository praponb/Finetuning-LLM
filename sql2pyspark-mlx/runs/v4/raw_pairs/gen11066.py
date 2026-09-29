from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")
w_running = Window.partitionBy("category").orderBy("cost", "id")
w_part = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc_nulls_last())
w4 = Window.partitionBy("category").orderBy("price", "id")
w5 = Window.partitionBy("category", "supplier_id").orderBy("stock_qty", "id").rowsBetween(-2, -1)

with_windows = (
    df_products
    .select(
        "*",
        F.ntile(5).over(w).alias("price_bucket"),
        F.ntile(4).over(w_running).alias("cost_bucket"),
        F.cume_dist().over(w_part).alias("stock_qty_cume_dist"),
        F.ntile(10).over(w4).alias("price_bucket_2"),
        F.avg("cost").over(w5).alias("cost_avg_2_prior"),
    )
)

result = (
    with_windows
    .groupBy(
        F.date_trunc("QUARTER", "launch_date").alias("quarter_start"),
        F.date_trunc("YEAR", "launch_date").alias("year_start"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.stddev_samp("stock_qty").alias("stock_qty_stddev"),
    )
    .orderBy("year_start")
)
