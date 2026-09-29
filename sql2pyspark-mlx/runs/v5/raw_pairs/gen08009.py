from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count("price").alias("price_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .filter(F.col("t.price_count") > 4)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.country"),
        F.col("t.price_count"),
    )
)
