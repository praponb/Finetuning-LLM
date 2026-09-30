from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .groupBy(
        F.when(F.col("p.price") >= 1040, "high").when(F.col("p.price") >= 260, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.sum(F.col("ss.revenue")).alias("total_revenue"),
        F.max(F.col("ss.revenue")).alias("max_revenue"),
    )
    .orderBy("total_revenue", "price_bucket")
    .limit(5)
)
