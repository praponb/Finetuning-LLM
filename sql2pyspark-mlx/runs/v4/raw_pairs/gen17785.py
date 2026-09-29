from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.revenue") > 1900)
    .groupBy(
        F.col("p.category"),
        F.when(F.col("p.brand").isin(["Initech", "Stark"]), "priority").otherwise("other").alias("brand_group"),
    )
    .agg(F.count("*").alias("store_sale_count"))
    .filter(F.col("store_sale_count") > 5)
    .orderBy("brand_group")
)
