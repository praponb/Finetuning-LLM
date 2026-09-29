from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "FedEx"]))
    .select(F.col("shipped_date").alias("event_date"))
    .unionByName(df_orders.select(F.col("order_date").alias("event_date")))
    .distinct()
)
