from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.category").isin(["Fashion", "Electronics", "Books"]))
    .groupBy(
        F.when(F.col("s.rating") >= 4.0, "high").when(F.col("s.rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.country"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.brand") == "Acme", 1).otherwise(0))) / F.count("*")), 1).alias("pct_acme"),
        F.max(F.col("p.price")).alias("max_price"),
    )
)
