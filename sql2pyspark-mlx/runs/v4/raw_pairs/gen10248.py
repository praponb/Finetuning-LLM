from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "right")
    .select(F.col("p.stock_qty"), F.col("s.rating"), F.col("s.country"))
)
