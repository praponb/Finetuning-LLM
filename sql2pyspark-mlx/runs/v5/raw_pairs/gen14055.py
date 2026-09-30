from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(
        F.col("s.city"),
        F.when(F.col("s.rating") >= 3.8, "high").when(F.col("s.rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.min(F.col("p.cost")).alias("min_cost"),
        F.max(F.col("p.launch_date")).alias("last_launch_date"),
    )
    .orderBy("rating_bucket", "city")
    .limit(5)
)
