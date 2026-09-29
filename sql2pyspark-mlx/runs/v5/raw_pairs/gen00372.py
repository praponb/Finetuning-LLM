from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(F.stddev("price"), 2).alias("stddev_price"),
        F.max("launch_date").alias("last_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .filter(F.col("ss.stddev_price") >= 27)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("ss.stddev_price"),
        F.col("ss.last_launch_date"),
    )
)
