from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.count("price").alias("price_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .filter(F.col("ss.price_count") >= 6)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("ss.last_launch_date"),
        F.col("ss.price_count"),
    )
    .orderBy("country")
)
