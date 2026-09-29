from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.count("stock_qty").alias("stock_qty_count"),
        (F.max("price") - F.min("price")).alias("price_range"),
    )
)
