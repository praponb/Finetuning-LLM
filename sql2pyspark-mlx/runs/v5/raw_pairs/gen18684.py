from pyspark.sql import functions as F

avg_shipping_fee = df_orders.filter(F.col("channel") == "partner").agg(F.avg("shipping_fee")).first()[0]

result = (
    df_orders
    .filter(F.col("shipping_fee") > avg_shipping_fee)
    .select("id", "shipping_fee", "order_date", "amount")
)
