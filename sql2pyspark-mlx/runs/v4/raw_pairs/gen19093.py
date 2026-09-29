from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & ~F.col("merchant").isin(["7-Eleven", "Shell", "Grab"]) & (F.col("merchant") == "Grab"))
    .groupBy("merchant")
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credit_amount"),
    )
    .orderBy(F.col("credit_amount").desc(), "merchant")
)
