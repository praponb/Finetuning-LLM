from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .groupBy(
        F.col("p.category"),
        F.when(F.col("p.price") >= 1010, "high").when(F.col("p.price") >= 520, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.max(F.col("ss.sale_date")).alias("latest_sale_date"),
        F.max(F.col("ss.revenue")).alias("max_revenue"),
    )
    .orderBy("category")
)
