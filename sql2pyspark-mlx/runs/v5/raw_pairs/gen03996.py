from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.lit("order").alias("kind"), "status")
    .unionByName(df_shipments.select(F.lit("shipment").alias("kind"), "status"))
)
