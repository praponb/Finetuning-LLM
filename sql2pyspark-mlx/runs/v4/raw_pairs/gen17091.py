from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") <= 980)
    .select(
        "amount",
        "method",
        F.when(F.col("method") == "paypal", 1).otherwise(0).alias("is_paypal"),
    )
)
