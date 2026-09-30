from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Grab", "7-Eleven"]) & (F.col("txn_type") != "DEBIT"))
    .select(
        "amount",
        F.dayofweek("txn_time").alias("txn_time_dayofweek"),
        F.col("amount").cast("int").alias("amount_int"),
    )
)
