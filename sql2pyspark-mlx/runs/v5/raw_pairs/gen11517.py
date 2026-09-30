from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("s.rating") >= 3.9, "high").when(F.col("s.rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.countDistinct(F.col("ss.units")).alias("unique_products"))
    .orderBy("rating_bucket", F.col("unique_products").desc())
)
