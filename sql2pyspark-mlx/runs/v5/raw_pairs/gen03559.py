from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") >= 560) & ~F.col("category").isin(["Home", "Books", "Sports"]) & (F.col("category") == "Home"))
    .select("id", "stock_qty", F.round((F.col("price") * 0.9), 2).alias("price_scaled"))
)
