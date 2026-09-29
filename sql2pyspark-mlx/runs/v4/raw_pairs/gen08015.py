from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy("stock_qty", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("stock_qty").desc())
w4 = Window.partitionBy("category", "supplier_id").orderBy("price_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("price_times_price_plus_stock_qty").desc())
w6 = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("category").orderBy(F.col("price_avg_2_prior").asc_nulls_first())
w8 = Window.partitionBy("supplier_id", "brand").orderBy("cumulative_price", "id").rowsBetween(-3, -1)
w9 = Window.partitionBy("brand")
w10 = Window.partitionBy("category", "brand")
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("cost").alias("sum_product_cost"))

valid = (
    df_products
    .filter(F.col("brand").isNotNull())
    .withColumn("price_plus_stock_qty", (F.col("price") + F.col("stock_qty")))
    .withColumn("stock_qty_level", F.when(F.col("stock_qty") >= 365, "High").when(F.col("stock_qty") >= 134, "Medium").otherwise("Low"))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("NONE")))
)

base = (
    valid
    .filter((F.col("launch_date") > F.lit("2024-03-12").cast("date")) & F.col("supplier_id").isin([10, 14, 16]))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "brand_clean",
        (F.col("price") * F.col("price_plus_stock_qty")).alias("price_times_price_plus_stock_qty"),
    )
)

with_windows = (
    base
    .withColumn("price_avg_2_prior", F.avg("price").over(w))
    .withColumn("first_known_price", F.first("price", ignorenulls=True).over(w2))
    .withColumn("stock_qty_cume_dist", F.cume_dist().over(w3))
)

kept = (
    with_windows
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") <= F.col("s.launch_date")), "left_anti")
)

with_windows_products = (
    kept
    .select(
        "*",
        F.sum("price").over(w4).alias("cumulative_price"),
        F.cume_dist().over(w5).alias("price_times_price_plus_stock_qty_cume_dist"),
        F.last("price_avg_2_prior", ignorenulls=True).over(w6).alias("last_known_price_avg_2_prior"),
    )
)

metrics = (
    with_windows_products
    .withColumn("price_avg_2_prior_rank", F.rank().over(w_ordered))
    .withColumn("first_known_price_avg_3_prior", F.avg("first_known_price").over(w8))
    .withColumn("first_known_price_share", F.round((F.col("first_known_price") / F.sum("first_known_price").over(w9)), 4))
    .withColumn("first_known_price_share_2", F.round((F.col("first_known_price") / F.sum("first_known_price").over(w10)), 4))
)

result = (
    metrics
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "launch_date",
        "tags",
        "supplier_id",
        "brand_clean",
        "price_times_price_plus_stock_qty",
        "price_avg_2_prior",
        F.col("sum_product_cost"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
