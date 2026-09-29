from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") > 1130)
    .select("status", F.lit("order").alias("source"))
    .unionByName(df_payments.select("status", F.lit("payment").alias("source")))
)
