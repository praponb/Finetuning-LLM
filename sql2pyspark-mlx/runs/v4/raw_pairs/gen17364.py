from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.max("cost").alias("max_cost"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("supplier_totals"), F.col("suppliers.id") == F.col("supplier_totals.supplier_id"), "inner")
    .select(F.col("suppliers.country"), F.col("supplier_totals.max_cost"))
)
