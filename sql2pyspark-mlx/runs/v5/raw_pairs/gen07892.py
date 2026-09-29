from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "outer")
    .select(F.col("ss.sale_date"), F.col("p.price"), F.col("s.sqft"), F.col("s.region"))
)
