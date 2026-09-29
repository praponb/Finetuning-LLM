from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("s.rating") >= 3.6, "high").when(F.col("s.rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.avg(F.col("p.price")).alias("avg_price"))
    .orderBy(F.col("avg_price").desc())
)
