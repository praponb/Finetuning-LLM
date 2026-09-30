from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("amount")).otherwise(0)).alias("bank_transfer_amount"),
    )
    .filter(F.col("bank_transfer_amount") >= 42)
    .select("order_id", "bank_transfer_amount")
    .orderBy(F.col("order_id").desc())
)
