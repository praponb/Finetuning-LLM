from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee")
    .subtract(df_payments.select(F.col("amount").alias("shipping_fee")))
    .orderBy("shipping_fee")
)
