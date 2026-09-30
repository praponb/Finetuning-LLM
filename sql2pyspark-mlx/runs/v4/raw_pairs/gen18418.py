from pyspark.sql import functions as F

max_shipping_fee = df_orders.filter(F.col("store_id") == 18).agg(F.max("shipping_fee")).first()[0]

result = (
    df_orders
    .filter(F.col("shipping_fee") >= max_shipping_fee)
    .select("id", "shipping_fee", "status")
    .orderBy(F.col("id").desc())
)
