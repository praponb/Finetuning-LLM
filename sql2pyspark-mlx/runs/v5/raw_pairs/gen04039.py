from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("txn_type").isin(["FEE", "DEBIT"]), "priority").otherwise("other").alias("txn_type_group"),
    )
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") > 2000)
    .orderBy("avg_amount")
)
