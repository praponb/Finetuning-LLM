from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Grocery")
    .groupBy("brand", "category")
    .agg(
        F.count("stock_qty").alias("stock_qty_count"),
        F.min("stock_qty").alias("min_stock_qty"),
    )
)
