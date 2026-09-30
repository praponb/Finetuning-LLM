from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter((F.col("s.country") == "TH") & (F.col("s.rating") <= 3.4))
    .select(
        F.col("s.country"),
        F.col("s.rating"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("p.price"),
    )
    .orderBy("rating")
)
