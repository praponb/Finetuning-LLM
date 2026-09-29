from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(31, 39))
    .select("status", F.lit("order").alias("source"))
    .unionByName(df_shipments.select("status", F.lit("shipment").alias("source")))
    .orderBy("source")
)
