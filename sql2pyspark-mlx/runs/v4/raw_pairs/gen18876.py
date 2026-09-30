from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") < 3700)
    .select("txn_type", "total_amount", "max_amount")
    .orderBy(F.col("max_amount").desc())
)
