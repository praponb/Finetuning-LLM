from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(F.countDistinct("amount").alias("unique_orders"))
)
