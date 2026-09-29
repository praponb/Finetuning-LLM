from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 6, 8, 10]) | (F.col("product_id") == 5))
    .groupBy(
        F.when(F.col("units") >= 36, "high").when(F.col("units") >= 14, "mid").otherwise("low").alias("units_bucket"),
        "product_id",
    )
    .agg(F.count("*").alias("num_store_sales"))
)
