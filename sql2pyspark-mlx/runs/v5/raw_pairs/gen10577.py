from pyspark.sql import functions as F

result = (
    df_orders
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_payments
        .filter(F.col("method") == "bank_transfer")
        .select("status", F.lit("payment").alias("source"))
    )
    .distinct()
)
