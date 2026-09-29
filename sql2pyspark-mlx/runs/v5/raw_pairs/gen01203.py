from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(~F.col("s.supplier_name").like("Pac%"))
    .select(F.col("s.supplier_name"), F.col("s.rating"), F.col("p.stock_qty"))
    .orderBy(F.col("stock_qty").desc(), F.col("supplier_name").desc())
)
