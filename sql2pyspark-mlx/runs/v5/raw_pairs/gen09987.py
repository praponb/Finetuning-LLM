from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(
        F.max("amount").alias("max_amount"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("max_amount") < 1500)
    .select("txn_year", "max_amount", "avg_amount")
)
