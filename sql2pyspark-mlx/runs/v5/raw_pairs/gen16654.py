from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") <= "2023-07-01")
    .groupBy(F.month("order_date").alias("order_month"))
    .agg((F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"))
    .filter(F.col("shipping_fee_range") > 67)
    .select("order_month", "shipping_fee_range")
)
