from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 38)
    .groupBy("brand")
    .agg(
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_count"),
    )
    .filter(F.col("sports_count") >= 5)
    .select("brand", "stock_qty_range", "sports_count")
    .orderBy("sports_count", "brand")
    .limit(1)
)
