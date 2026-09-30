from pyspark.sql import functions as F

result = (
    df_products
    .select("cost")
    .intersect(df_orders.filter(F.col("amount") > 460).select("shipping_fee"))
)
