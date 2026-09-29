from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 1510)
    .select(F.lit("order").alias("kind"), "amount")
    .union(df_shipments.select(F.lit("shipment").alias("kind"), "cost"))
)
