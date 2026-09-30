from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.stddev_cost") >= 75)
    .select(F.col("s.rating"), F.col("ss.first_launch_date"), F.col("ss.stddev_cost"))
    .orderBy("first_launch_date")
)
