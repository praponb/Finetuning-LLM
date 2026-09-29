from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .filter((F.col("s.rating") <= 2.3) & F.col("s.supplier_name").isNotNull())
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.date_trunc("YEAR", F.col("p.launch_date")).alias("launch_date_period"),
    )
)
