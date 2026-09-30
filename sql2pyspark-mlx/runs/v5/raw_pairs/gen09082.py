from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("units") >= 13).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_semi")
    .filter(F.month(F.col("p.launch_date")).isin([2, 3, 8]))
    .select(F.col("p.id"), F.col("p.price"), F.col("p.brand"))
)
