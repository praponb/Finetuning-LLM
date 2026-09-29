from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.city").isin(["Berlin", "Singapore", "Bangkok"]))
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("p.product_name"),
        F.col("s.city"),
        F.col("s.rating"),
        F.col("s.country"),
    )
)
