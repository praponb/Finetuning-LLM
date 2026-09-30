from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(1100, 1320))
    .groupBy("store_id")
    .agg(F.min("shipping_fee").alias("min_shipping_fee"))
    .filter(F.col("min_shipping_fee") < 29)
    .select("store_id", "min_shipping_fee")
    .orderBy(F.col("store_id").desc())
)
