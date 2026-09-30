from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 4, 6, 7]) & (F.col("units") == 31))
    .groupBy(
        F.when(F.col("units") >= 41, "high").when(F.col("units") >= 17, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.count("units").alias("units_count"),
        F.max("revenue").alias("max_revenue"),
    )
    .filter(F.col("max_revenue") > 3900)
)
