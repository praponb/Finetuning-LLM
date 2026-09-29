from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy("running_min_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id").orderBy("running_sum_stock_qty", "id")
w4 = Window.partitionBy("supplier_id")
w5 = Window.partitionBy("supplier_id").orderBy(F.col("running_count_stock_qty").desc(), "id")

base = (
    df_products
    .filter(F.month("launch_date").isin([1, 3, 6]))
    .withColumn("margin", F.round((F.col("price") / F.col("cost")), 2))
)

with_windows = (
    base
    .select(
        "id",
        "product_name",
        "stock_qty",
        "tags",
        "supplier_id",
        "margin",
        F.min("stock_qty").over(w).alias("running_min_stock_qty"),
        F.count("stock_qty").over(w).alias("running_count_stock_qty"),
        F.sum("stock_qty").over(w).alias("running_sum_stock_qty"),
    )
)

with_windows_products = (
    with_windows
    .withColumn("last_known_margin", F.last("margin", ignorenulls=True).over(w2))
    .withColumn("running_sum_stock_qty_bucket", F.ntile(10).over(w3))
    .withColumn("supplier_id_max_margin", F.max("margin").over(w4))
)

latest = (
    with_windows_products
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "product_name",
        "stock_qty",
        "tags",
        "supplier_id",
        "margin",
        "running_sum_stock_qty",
        "running_sum_stock_qty_bucket",
        "supplier_id_max_margin",
    )
)

result = (
    latest
    .alias("src")
    .join(df_suppliers.alias("dim"), F.col("src.supplier_id") == F.col("dim.id"), "left")
    .groupingSets(
        [["city", "country"], ["city"], []],
        F.col("dim.city").alias("city"),
        F.col("dim.country").alias("country"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("src.stock_qty")), 2).alias("total_stock_qty"),
        F.round(F.avg(F.col("src.running_sum_stock_qty")), 2).alias("avg_running_sum_stock_qty"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("rows_total").asc_nulls_last())
)
