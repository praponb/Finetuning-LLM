from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("cost") <= 450).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("ss.product_id").isin([5, 6, 7, 8]))
    .select(F.col("ss.id"), F.col("ss.sale_date"), F.col("ss.units"))
    .orderBy(F.col("units").desc())
)
