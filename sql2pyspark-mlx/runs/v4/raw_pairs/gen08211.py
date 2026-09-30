from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("supplier_id", "brand").orderBy("launch_date_period", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc_nulls_first())
w4 = Window.partitionBy("category", "brand").orderBy(F.col("launch_date_period").desc(), "id")
w5 = Window.partitionBy("category", "supplier_id").orderBy(F.col("price").desc_nulls_first())
w6 = Window.orderBy("cost_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("price_avg_5", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("supplier_id").isin([5, 8, 15, 34]))
    .select(
        "*",
        F.concat("product_name", F.lit(" - "), "category").alias("product_name_category"),
        F.date_trunc("week", "launch_date").alias("launch_date_period"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.ntile(5).over(w).alias("cost_bucket"),
        F.avg("price").over(w2).alias("price_avg_5"),
        F.percent_rank().over(w3).alias("stock_qty_percent_rank"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.row_number().over(w4).alias("event_seq"),
        F.dense_rank().over(w5).alias("price_dense_rank"),
        F.last("brand", ignorenulls=True).over(w6).alias("last_known_brand"),
        F.avg("price_avg_5").over(w7).alias("price_avg_5_avg_5"),
        F.sum("stock_qty").over(w8).alias("running_stock_qty"),
    )
)

result = (
    enriched
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "stock_qty",
        "tags",
        "product_name_category",
        "launch_date_period",
        "cost_bucket",
    )
)
