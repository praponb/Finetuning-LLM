from pyspark.sql import functions as F

src = (
    df_products
    .filter(F.array_contains("tags", "sale") & F.array_contains("tags", "clearance") & (F.col("category") == "Grocery"))
)

products_stats = (
    src
    .groupBy(
        F.date_trunc("MONTH", "launch_date").alias("launch_month"),
        F.when(F.col("stock_qty") >= 383, "high").when(F.col("stock_qty") >= 183, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_count"),
        F.round(F.stddev("price"), 2).alias("stddev_price"),
    )
)

result = (
    products_stats
    .filter(F.col("stddev_price") >= 54)
    .orderBy(F.col("electronics_count").desc())
)
