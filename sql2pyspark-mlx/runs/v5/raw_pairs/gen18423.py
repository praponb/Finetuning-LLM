from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("price") > 680)
    .groupBy("supplier_id")
    .agg(
        F.max("cost").alias("max_cost"),
        F.max("launch_date").alias("last_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .filter(F.col("ss.max_cost") >= 720)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.rating"),
        F.col("ss.max_cost"),
        F.col("ss.last_launch_date"),
    )
    .orderBy("last_launch_date")
)
