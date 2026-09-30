from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w_grp = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("running_launch_date_year").desc())
w5 = Window.orderBy(F.col("running_sum_stock_qty").asc_nulls_first())

base = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        F.year("launch_date").alias("launch_date_year"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date").alias("store_sale_sale_date"))
)

with_windows = (
    with_last
    .filter(F.col("cost") >= 400)
    .withColumn("running_max_stock_qty", F.max("stock_qty").over(w_grp))
    .withColumn("running_min_stock_qty", F.min("stock_qty").over(w_grp))
    .withColumn("running_sum_stock_qty", F.sum("stock_qty").over(w_grp))
    .withColumn("running_launch_date_year", F.sum("launch_date_year").over(w3))
)

remaining_keys = (
    with_windows
    .select("category", "brand")
    .intersect(
        df_products
        .filter(F.col("supplier_id").isNull())
        .select("category", "brand")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.category") == F.col("k.category")) & (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "brand",
        "launch_date",
        "launch_date_year",
        "running_min_stock_qty",
        "running_sum_stock_qty",
        F.cume_dist().over(w4).alias("running_launch_date_year_cume_dist"),
        F.percent_rank().over(w5).alias("running_sum_stock_qty_percent_rank"),
    )
)
