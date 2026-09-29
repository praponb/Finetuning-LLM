from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["LOST", "DELIVERED", "IN_TRANSIT"]) & (F.col("order_id").isNull() | F.col("order_id").isNull()))
    .select("id", "order_id", "carrier", F.lower("carrier").alias("carrier_lower"))
    .orderBy("carrier")
)
