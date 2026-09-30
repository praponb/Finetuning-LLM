from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(1400, 2600) & (F.col("product_id").isin([1, 8]) | (F.col("revenue") > 2100)))
    .groupBy(
        F.when(F.col("units") >= 38, "high").when(F.col("units") >= 11, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.max("revenue").alias("max_revenue"),
        F.sum("revenue").alias("total_revenue"),
    )
    .filter(F.col("total_revenue") < 15000)
    .drop("total_revenue")
)
