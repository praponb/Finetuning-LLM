from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .groupBy(
        F.when(F.col("s.rating") >= 3.5, "high").when(F.col("s.rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"))
    .filter(F.col("stddev_revenue") > 26)
    .orderBy("rating_bucket")
    .limit(1)
)
