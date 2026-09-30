from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee")
    .unionByName(df_products.select(F.col("price").alias("shipping_fee")))
)
