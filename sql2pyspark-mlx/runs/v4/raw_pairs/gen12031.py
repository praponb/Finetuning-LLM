from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("is_flagged"))
    .withColumn("amount_whole", F.ceil("amount"))
)

agg = (
    base
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(F.max("txn_time").alias("latest_txn_time"))
)

result = agg.orderBy(F.col("txn_cohort").desc(), "latest_txn_time")
