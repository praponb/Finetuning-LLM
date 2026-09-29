from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & F.hour("txn_time").between(11, 18))
    .groupBy(
        F.month("txn_time").alias("txn_month"),
        F.quarter("txn_time").alias("txn_quarter"),
    )
    .agg(
        F.min("amount").alias("min_amount"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy("txn_quarter")
)
