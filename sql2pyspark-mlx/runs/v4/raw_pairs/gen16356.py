from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .select(F.col("ss.id"), F.col("ss.units"), F.col("p.category"))
    .orderBy(F.col("id").desc())
    .limit(50)
)
