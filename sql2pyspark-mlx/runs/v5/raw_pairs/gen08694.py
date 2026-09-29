from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") > 700) & F.col("status").isin(["FAILED", "SUCCESS"]))
    .select(
        "id",
        "amount",
        "paid_at",
        F.when(F.col("method") == "paypal", 1).otherwise(0).alias("is_paypal"),
    )
)
