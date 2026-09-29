from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost").between(49, 88) & (F.col("status") == "RETURNED")) | F.col("order_id").isin([2, 30]))
    .withColumn("delivered_date_period", F.date_trunc("WEEK", "delivered_date"))
)
