from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id").orderBy("amount", "id")
w2 = Window.partitionBy("merchant").orderBy(F.col("amount").asc_nulls_last())

enriched = (
    df_transactions
    .withColumn("amount_bucket", F.ntile(3).over(w))
    .withColumn("amount_rank", F.rank().over(w2))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("amount_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("amount_rank_tier"),
    )
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("amount_rank").alias("avg_amount_rank"),
        F.min("txn_time").alias("first_txn_time_2"),
    )
)
