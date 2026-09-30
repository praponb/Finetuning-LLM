from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.count("price").alias("price_count"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.home_count") > 6)
    .select(F.col("s.supplier_name"), F.col("t.price_count"), F.col("t.home_count"))
)
