from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "left")
    .groupBy(
        F.when(F.col("suppliers.rating") >= 4.4, "high").when(F.col("suppliers.rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
        F.col("suppliers.city"),
    )
    .agg(F.max(F.col("products.launch_date")).alias("last_launch_date"))
)
