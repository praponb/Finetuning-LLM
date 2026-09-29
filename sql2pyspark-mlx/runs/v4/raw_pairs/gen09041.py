from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Home", 1).otherwise(0)).alias("home_count"),
        F.max("price").alias("max_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .select(F.col("s.city"), F.col("ss.home_count"), F.col("ss.max_price"))
)
