from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id").orderBy(F.col("amount").desc())

ranked = (
    df_transactions
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .rollup("txn_type")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("amount"))).alias("starbucks_total_amount"),
        F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"),
        F.avg("amount").alias("avg_amount_2"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.grouping("txn_type").alias("grouping_id"),
    )
)
