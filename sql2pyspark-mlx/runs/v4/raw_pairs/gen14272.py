from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "PENDING") & (F.col("store_id").isin([21, 33]) | (F.col("store_id") == 8)))
    .withColumn("shipping_fee_filled", F.coalesce("shipping_fee", F.lit(0)))
    .withColumn("amount_adjusted", F.round((F.col("amount") * 1.15), 2))
)
