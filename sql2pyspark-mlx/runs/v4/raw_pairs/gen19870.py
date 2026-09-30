from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.country") != "US")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("s.rating"),
        F.col("s.country"),
        F.coalesce(F.col("s.rating"), F.lit(0)).alias("rating_filled"),
    )
    .orderBy("id")
)
