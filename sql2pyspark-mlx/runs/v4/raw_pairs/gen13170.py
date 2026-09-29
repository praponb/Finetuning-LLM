from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date", F.lit("order").alias("source"))
    .union(df_shipments.select("shipped_date", F.lit("shipment").alias("source")))
    .distinct()
)
