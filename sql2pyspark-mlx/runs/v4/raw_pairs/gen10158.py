from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.count("*").alias("product_count"),
        F.count("price").alias("price_count"),
        F.sum(F.when(F.col("category") == "Home", F.col("stock_qty")).otherwise(0)).alias("home_stock_qty"),
    )
)
