from pyspark.sql import functions as F

result = (
    df_products
    .filter(((F.col("cost") > 410) & F.array_contains("tags", "premium")) | F.col("supplier_id").isNotNull())
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
        F.sum(F.when(F.col("category") == "Sports", F.col("stock_qty")).otherwise(0)).alias("sports_stock_qty"),
    )
    .orderBy(F.col("sports_stock_qty").desc())
)
