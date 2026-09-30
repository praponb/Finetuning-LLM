from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.size("tags") > 2)
    .groupBy("supplier_id")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("supplier_stats"), F.col("suppliers.id") == F.col("supplier_stats.supplier_id"), "inner")
    .filter(F.col("supplier_stats.avg_cost") > 72)
    .select(F.col("suppliers.supplier_name"), F.col("supplier_stats.avg_cost"))
    .orderBy("supplier_name")
)
