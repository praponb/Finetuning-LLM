from pyspark.sql import functions as F

result = (
    df_order_items
    .select(F.lit("order_item").alias("kind"), "unit_price")
    .union(df_payments.select(F.lit("payment").alias("kind"), "amount"))
)
