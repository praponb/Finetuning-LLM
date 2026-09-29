from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id", "merchant")
    .pivot("txn_type", ["DEBIT", "CREDIT", "FEE"])
    .agg(F.max("amount").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "account_id",
        "merchant",
        F.col("DEBIT_amount").alias("debit_amount"),
        F.col("DEBIT_n").alias("debit_n"),
        F.col("CREDIT_amount").alias("credit_amount"),
        F.col("CREDIT_n").alias("credit_n"),
        F.col("FEE_amount").alias("fee_amount"),
        F.col("FEE_n").alias("fee_n"),
    )
)
