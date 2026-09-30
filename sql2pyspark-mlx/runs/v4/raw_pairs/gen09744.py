from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("channel").isin(["store", "web"]))
    .select("order_date", "store_id", "shipping_fee")
)
