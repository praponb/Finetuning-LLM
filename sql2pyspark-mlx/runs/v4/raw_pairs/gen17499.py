from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("cost").between(330, 620))
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0))) / F.count("*")), 1).alias("pct_initech"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("supplier_stats"), F.col("suppliers.id") == F.col("supplier_stats.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.supplier_name"),
        F.col("supplier_stats.min_launch_date"),
        F.col("supplier_stats.pct_initech"),
    )
    .orderBy("supplier_name", "id")
    .limit(10)
)
