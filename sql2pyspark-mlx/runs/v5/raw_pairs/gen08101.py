from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Electronics", 1).otherwise(0)).alias("electronics_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "inner")
    .filter(F.col("S.electronics_count") >= 32)
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("S.electronics_count"))
    .orderBy("electronics_count")
)
