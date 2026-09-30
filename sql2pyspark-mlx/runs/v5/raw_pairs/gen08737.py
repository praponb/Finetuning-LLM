from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("supplier_id") == 38)
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0)).alias("globex_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .filter(F.col("ss.globex_count") > 71)
    .select(F.col("s.id"), F.col("s.country"), F.col("ss.globex_count"))
    .orderBy(F.col("globex_count").desc())
)
