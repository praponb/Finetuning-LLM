from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(F.max("launch_date").alias("last_launch_date"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("ss.last_launch_date"))
    .orderBy("last_launch_date", F.col("id").desc())
    .limit(5)
)
