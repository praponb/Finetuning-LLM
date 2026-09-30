from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", F.col("stock_qty")).otherwise(0)).alias("globex_stock_qty"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("supplier_totals"), F.col("suppliers.id") == F.col("supplier_totals.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.city"),
        F.col("suppliers.supplier_name"),
        F.col("supplier_totals.globex_stock_qty"),
    )
    .orderBy(F.col("supplier_name").desc())
)
