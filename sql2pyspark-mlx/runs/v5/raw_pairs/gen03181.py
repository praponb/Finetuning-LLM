from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.price") <= 360)
    .groupBy(F.col("s.city"), F.col("s.country"))
    .agg(
        F.countDistinct(F.col("p.price")).alias("unique_suppliers"),
        F.max(F.col("p.launch_date")).alias("latest_launch_date"),
    )
)
