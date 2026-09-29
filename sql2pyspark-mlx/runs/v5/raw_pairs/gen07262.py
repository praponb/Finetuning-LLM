from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("t.latest_launch_date"),
        F.col("t.avg_cost"),
    )
)
