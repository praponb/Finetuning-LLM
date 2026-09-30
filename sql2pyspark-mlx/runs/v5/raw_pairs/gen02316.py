from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") < 25)
    .agg(F.min("amount").alias("min_amount"))
)
