from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.array_contains("tags", "premium")).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("ss.units").between(13, 33))
    .select(F.col("ss.id"), F.col("ss.product_id"), F.col("ss.sale_date"))
    .orderBy(F.col("id").desc())
)
