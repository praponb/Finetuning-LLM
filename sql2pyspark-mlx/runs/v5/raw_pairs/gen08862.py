from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id").orderBy("launch_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy(F.col("cost").desc())
w3 = Window.partitionBy("supplier_id", "category").orderBy(F.col("last4_max_stock_qty").desc_nulls_first())
w4 = Window.partitionBy("brand").orderBy(F.col("cost").desc_nulls_first())
w5 = Window.partitionBy("category")
w6 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc())
w7 = Window.partitionBy("supplier_id")

with_windows = (
    df_products
    .filter(F.col("launch_date") <= "2024-10-26")
    .select(
        "*",
        F.min("stock_qty").over(w).alias("last4_min_stock_qty"),
        F.max("stock_qty").over(w).alias("last4_max_stock_qty"),
        F.avg("stock_qty").over(w).alias("last4_avg_stock_qty"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.quantity").between(4, 7)), "left_anti")
)

matched_keys = (
    not_flagged
    .select("brand")
    .intersect(
        df_products
        .filter(F.col("launch_date") <= "2024-09-01")
        .select("brand")
    )
)

reconciled = (
    not_flagged
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

with_windows_products = (
    reconciled
    .withColumn("cost_cume_dist", F.cume_dist().over(w2))
    .withColumn("last4_max_stock_qty_percent_rank", F.percent_rank().over(w3))
    .withColumn("cost_rank", F.rank().over(w4))
    .withColumn("category_stddev_samp_price", F.stddev_samp("price").over(w5))
    .withColumn("category_count_last4_min_stock_qty", F.count("last4_min_stock_qty").over(w5))
)

scored = (
    with_windows_products
    .select(
        "*",
        F.cume_dist().over(w6).alias("stock_qty_cume_dist"),
        F.corr("price", "stock_qty").over(w7).alias("corr_price_stock_qty"),
    )
)

result = (
    scored
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "last4_min_stock_qty",
        "last4_max_stock_qty",
        "last4_max_stock_qty_percent_rank",
        "category_stddev_samp_price",
        "category_count_last4_min_stock_qty",
        "stock_qty_cume_dist",
        F.round((F.col("category_stddev_samp_price") / F.sum("category_stddev_samp_price").over(w5)), 4).alias("category_stddev_samp_price_share"),
    )
)
