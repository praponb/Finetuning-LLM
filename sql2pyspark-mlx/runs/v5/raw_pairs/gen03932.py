from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").asc_nulls_last(), "id")
w2 = Window.orderBy(F.col("line_total").desc(), "id")
w_running = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") > 160))
    .withColumn("line_total", (F.col("quantity") + F.col("unit_price")))
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "product_id", "quantity", "unit_price", "line_total")
)

with_windows = (
    latest
    .withColumn("line_total_bucket", F.ntile(4).over(w2))
    .withColumn("line_total_avg_5", F.avg("line_total").over(w_running))
)

valid = (
    with_windows
    .filter(F.col("line_total_avg_5") != 67)
    .withColumn("line_total_bucket_minus_unit_price", (F.col("line_total_bucket") - F.col("unit_price")))
)

enriched = (
    valid
    .filter(F.col("quantity") < 5)
    .select(
        "*",
        F.sum("line_total_bucket").over(w4).alias("running_sum_line_total_bucket"),
        F.min("line_total_bucket").over(w4).alias("running_min_line_total_bucket"),
        F.avg("line_total_bucket").over(w4).alias("running_avg_line_total_bucket"),
        F.count("line_total_bucket").over(w4).alias("running_count_line_total_bucket"),
    )
)

result = (
    enriched
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .cube(F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("a.quantity")), 2).alias("avg_quantity"),
        F.max(F.col("a.unit_price")).alias("max_unit_price"),
    )
)
