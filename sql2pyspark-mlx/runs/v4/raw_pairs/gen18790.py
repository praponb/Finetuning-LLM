from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.count("cost").alias("cost_count"),
        F.sum(F.when(F.col("brand") == "Globex", F.col("price")).otherwise(0)).alias("globex_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.globex_price") >= 49)
    .select(
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("ss.cost_count"),
        F.col("ss.globex_price"),
    )
    .orderBy(F.col("cost_count").desc())
)
