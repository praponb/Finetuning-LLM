from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 4.2, "high").when(F.col("s.rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct(F.col("p.price")).alias("unique_suppliers"),
        F.round(F.avg(F.col("p.stock_qty")), 2).alias("avg_stock_qty"),
    )
    .filter(F.col("avg_stock_qty") > 68)
)
