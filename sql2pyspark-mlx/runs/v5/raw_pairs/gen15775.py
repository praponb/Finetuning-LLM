from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 39)
    .select(F.lit("order").alias("source"), "status")
    .unionByName(df_payments.select(F.lit("payment").alias("source"), "status"))
    .distinct()
)
