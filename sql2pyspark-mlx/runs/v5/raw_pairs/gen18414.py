from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(28, 94))
    .select("cost", F.lit("shipment").alias("source"))
    .union(df_orders.select("shipping_fee", F.lit("order").alias("source")))
    .orderBy("source")
)
