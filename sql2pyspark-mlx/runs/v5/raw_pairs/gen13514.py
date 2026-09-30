from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.col("rating") > 2.2).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.col("p.launch_date"),
    )
    .orderBy(F.col("stock_qty").desc())
)
