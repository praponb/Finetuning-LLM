from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").isNotNull())
    .groupBy(
        F.when(F.col("txn_type").isin(["DEBIT", "FEE"]), "group_a").otherwise("other").alias("txn_type_group"),
        F.quarter("txn_time").alias("txn_quarter"),
    )
    .agg(F.count("amount").alias("amount_count"))
)
