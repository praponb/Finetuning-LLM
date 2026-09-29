from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(
        F.sum(F.when(F.col("merchant") == "Shell", F.col("amount")).otherwise(0)).alias("shell_amount"),
    )
    .filter(F.col("shell_amount") < 29)
    .select("txn_year", "shell_amount")
    .orderBy(F.col("shell_amount").desc(), "txn_year")
)
