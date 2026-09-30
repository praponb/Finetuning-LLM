from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["partner", "mobile", "store"]) & (F.col("discount") <= 0.29))
    .agg(F.min("order_date").alias("first_order_date"))
)
