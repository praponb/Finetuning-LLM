from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.max("launch_date").alias("latest_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "inner")
    .select(
        F.col("s.rating"),
        F.col("S.min_launch_date"),
        F.col("S.latest_launch_date"),
    )
)
