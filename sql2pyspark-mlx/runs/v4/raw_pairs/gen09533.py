from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("row_count").desc())

result = (
    df_transactions
    .groupBy(
        "account_id",
        F.when(F.col("amount") >= 4000, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.max(F.when(F.col("txn_type") == "CREDIT", F.col("amount"))).alias("credit_max_amount"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credit_amount"),
        F.count("*").alias("row_count"),
    )
    .withColumn("row_count_rank", F.rank().over(w))
)
