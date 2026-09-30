from pyspark.sql import functions as F

result = (
    df_orders
    .withColumn("channel_status_key", F.concat_ws("-", "channel", "status"))
    .withColumn("least_shipping_fee_discount", F.least("shipping_fee", "discount"))
)
