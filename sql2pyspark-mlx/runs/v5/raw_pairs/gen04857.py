from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.array_contains("tags", "sale"))
    .groupBy("supplier_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .filter(F.col("ss.total_count") > 6)
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("ss.total_count"))
)
