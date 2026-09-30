from pyspark.sql import functions as F

valid_orders = df_orders.filter(F.col("amount") < 1120)

agg = (
    valid_orders
    .groupBy("status")
    .agg(
        F.min("shipping_fee").alias("min_shipping_fee"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = agg.filter(F.col("amount_range") >= 23)
