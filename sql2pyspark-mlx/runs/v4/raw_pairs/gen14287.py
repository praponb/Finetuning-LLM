from pyspark.sql import functions as F

result = (
    df_products
    .select("cost")
    .intersect(df_orders.select(F.col("shipping_fee").alias("cost")))
    .orderBy("cost")
)
