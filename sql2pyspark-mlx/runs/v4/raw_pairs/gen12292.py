from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") <= 430)
    .select(
        "launch_date",
        "product_name",
        F.floor("stock_qty").alias("stock_qty_whole"),
        F.size("tags").alias("tags_count"),
    )
)
