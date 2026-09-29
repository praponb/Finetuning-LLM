from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("ss.revenue").isNull() & F.col("ss.product_id").isin([2, 4, 7, 10]))
    .select(F.col("p.launch_date"), F.col("ss.units"), F.col("ss.revenue"))
    .orderBy(F.col("revenue").desc())
)
