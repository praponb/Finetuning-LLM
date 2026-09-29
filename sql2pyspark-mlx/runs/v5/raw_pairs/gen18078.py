from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.city") == "New York")
    .groupBy(
        F.when(F.col("p.price") >= 1350, "high").when(F.col("p.price") >= 380, "mid").otherwise("low").alias("price_bucket"),
        F.when(F.col("p.brand").isin(["Umbrella", "Initech"]), "core").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"),
        F.min(F.col("ss.sale_date")).alias("first_sale_date"),
    )
)
