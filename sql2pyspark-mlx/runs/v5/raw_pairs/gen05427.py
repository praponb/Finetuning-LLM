from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(F.min("launch_date").alias("first_launch_date"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "left")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.rating"),
        F.col("t.first_launch_date"),
    )
)
