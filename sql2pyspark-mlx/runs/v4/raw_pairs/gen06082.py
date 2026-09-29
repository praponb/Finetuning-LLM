from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .groupBy(
        F.when(F.col("products.cost") >= 570, "high").when(F.col("products.cost") >= 310, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.count("*").alias("total_count"))
    .orderBy("total_count")
)
