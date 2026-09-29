from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(37, 39))
    .select(F.col("amount").alias("value"))
    .unionByName(df_shipments.select(F.col("cost").alias("value")))
    .orderBy("value")
)
