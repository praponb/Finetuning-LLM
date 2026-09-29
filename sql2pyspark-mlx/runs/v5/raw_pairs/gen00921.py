from pyspark.sql import functions as F

result = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("c.tags"), "bestseller")), "left_anti")
)
