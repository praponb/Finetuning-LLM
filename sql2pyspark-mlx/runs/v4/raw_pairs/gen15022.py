from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") < 14)
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_payments
        .filter(F.col("amount") <= 970)
        .select("status", F.lit("payment").alias("source"))
    )
    .distinct()
)
