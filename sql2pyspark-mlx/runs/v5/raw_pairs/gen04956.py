from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.fashion_count") > 4)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.country"),
        F.col("t.latest_launch_date"),
        F.col("t.fashion_count"),
    )
)
