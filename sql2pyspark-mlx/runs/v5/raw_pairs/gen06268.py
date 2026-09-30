from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.array_contains("tags", "clearance"))
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Stark", F.col("price")).otherwise(0)).alias("stark_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.stark_price") >= 78)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("ss.stark_price"),
    )
    .orderBy("stark_price", F.col("id").desc())
    .limit(5)
)
