from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.supplier_id").isin([5, 11, 26]))
    .groupBy(
        F.when(F.col("s.rating") >= 4.0, "high").when(F.col("s.rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("p.category") == "Sports", F.col("p.stock_qty")).otherwise(0)).alias("sports_stock_qty"),
    )
)
