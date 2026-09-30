from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 3.5, "high").when(F.col("s.rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.round(F.stddev(F.col("p.price")), 2).alias("stddev_price"))
    .filter(F.col("stddev_price") > 22)
    .orderBy(F.col("stddev_price").desc(), "rating_bucket")
    .limit(5)
)
