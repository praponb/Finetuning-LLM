from pyspark.sql import functions as F

base = df_transactions.filter(F.col("merchant").isin(["Shell", "7-Eleven"]))

transactions_stats = (
    base
    .groupBy(
        F.when(F.col("amount") >= 3600, "high").when(F.col("amount") >= 1400, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
)

result = (
    transactions_stats
    .filter(F.col("avg_amount") >= 3900)
    .select("amount_bucket", "avg_amount", "txn_type_list")
    .orderBy("amount_bucket", F.col("avg_amount").desc())
)
