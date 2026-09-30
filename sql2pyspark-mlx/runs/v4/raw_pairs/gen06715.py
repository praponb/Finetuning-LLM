from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .groupBy(
        F.when(F.col("p.cost") >= 630, "high").when(F.col("p.cost") >= 210, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.count("*").alias("num_store_sales"),
        F.min(F.col("ss.sale_date")).alias("min_sale_date"),
    )
    .orderBy(F.col("num_store_sales").desc())
)
