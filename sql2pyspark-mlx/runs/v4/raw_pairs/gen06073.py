from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .groupBy(
        F.when(F.col("suppliers.rating") >= 3.6, "high").when(F.col("suppliers.rating") >= 2.6, "mid").otherwise("low").alias("rating_bucket"),
        F.col("suppliers.country"),
    )
    .agg(F.countDistinct(F.col("store_sales.units")).alias("unique_products"))
    .orderBy("rating_bucket", F.col("country").desc())
    .limit(5)
)
