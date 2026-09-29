from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .groupBy(
        F.when(F.col("suppliers.rating") >= 3.5, "high").when(F.col("suppliers.rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
        F.col("suppliers.country"),
    )
    .agg(
        F.min(F.col("products.price")).alias("min_price"),
        F.count(F.col("products.cost")).alias("cost_count"),
    )
    .orderBy("country")
)
