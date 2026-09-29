from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(F.min("launch_date").alias("first_launch_date"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .select(F.col("s.city"), F.col("s.country"), F.col("ss.first_launch_date"))
    .orderBy("first_launch_date")
)
