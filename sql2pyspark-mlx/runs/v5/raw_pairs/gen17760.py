from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Grocery", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grocery"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(F.col("s.rating"), F.col("s.city"), F.col("t.pct_grocery"))
)
