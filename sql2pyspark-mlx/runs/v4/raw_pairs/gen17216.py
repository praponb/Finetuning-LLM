from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.rating") > 4.2)
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.cost"),
        F.col("p.stock_qty"),
        F.col("s.rating"),
        F.col("s.city"),
    )
    .orderBy(F.col("brand").desc(), "id")
    .limit(20)
)
