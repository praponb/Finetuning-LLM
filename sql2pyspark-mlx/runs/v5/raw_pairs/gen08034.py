from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .groupBy(
        F.when(F.col("s.sqft") >= 13881, "high").when(F.col("s.sqft") >= 4424, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.max(F.col("ss.sale_date")).alias("latest_sale_date"))
)
