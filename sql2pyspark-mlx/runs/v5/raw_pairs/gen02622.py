from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("carrier_length", F.length("carrier"))
    .withColumn("delivered_date_period", F.date_trunc("MONTH", "delivered_date"))
    .orderBy("order_id", F.col("carrier_length").desc())
)
