from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy(F.col("stock_qty").desc(), "id")
w_part = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("category", "brand")
w_part_2 = Window.partitionBy("brand", "category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("brand").orderBy("rn", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("category", "brand").orderBy("last_known_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("running_stock_qty_2", "id")
w8 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("last_known_stock_qty", "id").rowsBetween(-6, Window.currentRow)
w10 = Window.orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("last_known_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy(F.col("category_sum_stock_qty").desc(), "id")

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.size("tags") > 1))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "stock_qty",
        "tags",
        F.upper("product_name").alias("product_name_upper"),
    )
)

ranked = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.sum("stock_qty").over(w_part).alias("running_stock_qty"),
        F.sum("stock_qty").over(w3).alias("category_sum_stock_qty"),
        F.last("stock_qty", ignorenulls=True).over(w_part_2).alias("last_known_stock_qty"),
        F.sum("stock_qty").over(w_part).alias("running_stock_qty_2"),
        F.avg("stock_qty").over(w5).alias("stock_qty_avg_2"),
    )
)

scored_products = (
    scored
    .select(
        "id",
        "tags",
        "product_name_upper",
        "rn",
        "category_sum_stock_qty",
        "last_known_stock_qty",
        "running_stock_qty_2",
        "stock_qty_avg_2",
        F.sum("running_stock_qty").over(w6).alias("cumulative_running_stock_qty"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

metrics = (
    scored_products
    .select(
        "id",
        "tags",
        "product_name_upper",
        "category_sum_stock_qty",
        "last_known_stock_qty",
        "row_seq",
        F.first("last_known_stock_qty", ignorenulls=True).over(w8).alias("first_known_last_known_stock_qty"),
        F.avg("cumulative_running_stock_qty").over(w9).alias("cumulative_running_stock_qty_avg_6"),
        F.first("cumulative_running_stock_qty", ignorenulls=True).over(w10).alias("first_known_cumulative_running_stock_qty"),
        F.last("last_known_stock_qty", ignorenulls=True).over(w11).alias("last_known_last_known_stock_qty"),
        F.ntile(4).over(w_grp).alias("category_sum_stock_qty_bucket"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("first_known_cumulative_running_stock_qty") >= 8696, "PREMIUM").otherwise("STANDARD").alias("first_known_cumulative_running_stock_qty_tier"),
        F.when(F.col("last_known_last_known_stock_qty") >= 395, "LARGE").otherwise("SMALL").alias("last_known_last_known_stock_qty_tier"),
        F.when(F.col("first_known_last_known_stock_qty") >= 402, "LARGE").otherwise("SMALL").alias("first_known_last_known_stock_qty_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("last_known_last_known_stock_qty"), 2).alias("avg_last_known_last_known_stock_qty"),
        F.count("*").alias("row_count_2"),
    )
    .filter(F.col("avg_last_known_last_known_stock_qty") > 0)
)
