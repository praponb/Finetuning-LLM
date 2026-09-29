from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.array_contains("tags", "sale"))
    .groupBy("supplier_id")
    .agg(F.sum("price").alias("total_price"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_summary.alias("supplier_summary"), F.col("suppliers.id") == F.col("supplier_summary.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.supplier_name"),
        F.col("supplier_summary.total_price"),
    )
    .orderBy(F.col("total_price").desc())
)
