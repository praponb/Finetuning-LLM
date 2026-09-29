from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "store", "web"]))
    .select("amount", "shipping_fee", "store_id")
)
