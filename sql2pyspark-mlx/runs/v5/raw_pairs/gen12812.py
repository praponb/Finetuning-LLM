from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.stock_qty").isNotNull() & F.col("s.rating").between(2.1, 2.2))
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.stock_qty"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
    )
    .orderBy("rating")
)
