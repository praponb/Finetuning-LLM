from pyspark.sql import functions as F

result = (
    df_products
    .select(F.lit("product").alias("source"), "cost")
    .union(df_orders.select(F.lit("order").alias("source"), "shipping_fee"))
    .distinct()
)
