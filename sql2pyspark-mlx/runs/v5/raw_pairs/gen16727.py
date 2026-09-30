from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.sqft") >= 17580, "high").when(F.col("s.sqft") >= 6174, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") >= 2)
    .orderBy("sqft_bucket", F.col("total_count").desc())
)
